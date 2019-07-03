from __future__ import print_function

import sys, re, logging

from hdfs import Hdfs

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel  import StorageLevel


class Spark:
    def __init__(self,log : RootLogger, jobConf: dict):
        self.log      = log        
        self.__initFlags(appdefaults = jobConf['appconfs']['appdefaults'])
        self.__spark = self.__setupSparkSession__(jobConf = jobConf)

        self.__dfltRDDParts = \
                int(self.__spark.conf.get('spark.executor.instances', '20')) * \
                int(self.__.conf.get('spark.executor.cores', '4')) * 2

        self.__hdfs = Hdfs(log= self.log, hadoop= self.__spark.sparkContext._jvm.org.apache.hadoop)

    def __initFlags(self, appdefaults):
        '''
        Init the job level parameters needed by this class
        '''
        self.__explainDF  = appdefaults.get('explaindf', False ) 
        self.__printcount = appdefaults.get('printdfcounts', False ) 
        self.__useHist    = appdefaults.get('usesqlhints', False ) 

        self.__saveDFAs   = appdefaults.get('savedfas', 'NONE' ).toUpperCase()
        self.__saveDF     = True if self.__saveDFAs != "NONE" else False  

        self.__fileFmt    = appdefaults.get('defaultfileformat','parquet').toLowerCase()

    def __setupSparkSession__(self,jobConf: dict, ) -> SparkSession:
        '''
        Init the Spark environemnt with few default configurations and start the spark session.
        '''
        conf = SparkConf()
       # 
       #Setup Spark Specific configurations 
       #         
        hmConf = {
            "spark.executor.pyspark.memory"                  : "512m",
            "spark.debug.maxToStringFields"                  : "5000",
            "spark.rps.askTimeout"                           : "1200",
            "spark.network.timeout"                          : "1200",

            "spark.maxRemoteBlockSizeFetchToMem"             : "512m",
            "spark.broadcast.blockSize"                      : "16m",
            "spark.broadcast.compress"                       : "true",
            "spark.rdd.compress"                             : "true",
            "spark.io.compression.codec"                     : "org.apache.spark.io.SnappyCompressionCodec",

            "spark.kryo.unsafe"                              : "true",
            "spark.serializer"                               : "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer"                    : "10240",
            "spark.kryoserializer.buffer.max"                : "2040m",
            "hive.exec.dynamic.partition"                    : "true",
            "hive.exec.dynamic.partition.mode"               : "nonstrict",
            "hive.warehouse.data.skiptrash"                  : "true",

            "spark.sql.hive.metastorePartitionPruning"       : "true",

            "spark.sql.broadcastTimeout"                                    : "1200",
            "spark.sql.sources.partitionOverwriteMode"                      : "dynamic",

            "spark.sql.orc.filterPushdown"                                  : "true",
            "spark.sql.orc.splits.include.file.footer"                      : "true",
            "spark.sql.orc.cache.stripe.details.size"                       : "1000",

            "spark.hadoop.parquet.enable.summary-metadata"                  : "false",
            "spark.sql.parquet.mergeSchema"                                 : "false",
            "spark.sql.parquet.filterPushdown"                              : "true",
            "spark.sql.parquet.fs.optimized.committer.optimization-enabled" : "true",            

            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"       :"2",
            "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true"
        }
        
        for (k,v) in  jobConf['sparkconfs'].items() :
            hmConf.set(k,v)

        conf.setAll(hmConf)
       # 
       #Setup Hadoop Specific configurations 
       # 
        hdpCnf = SparkContext._jsc.hadoopConfiguration()
        hdpCnf.set('io.file.buffer.size', '65536')
        hdpCnf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

        for (k,v) in  jobConf['hadoopconfs'].items() :
            hdpCnf.set(k,v)

       # 
       #Setup AWS Specific configurations 
       # 
        if jobConf['appconfs']['runenv'].toUpperCase() == 'AWS' :            
            SparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
            SparkContext.setSystemProperty('com.amazonaws.services.s3.enforceV4', 'true')
            conf.set("spark.sql.parquet.output.committer.class","com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter")

            cred = None
            try:
                from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
                provider = InstanceMetadataProvider(iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2))
                creds = provider.load()
                hdpCnf.setAll({
                    'fs.s3a.access.key' : creds.access_key,
                    'fs.s3a.access.key' : creds.secret_key,
                })                
            except:
                pass
            hdpCnf.setAll({
                'fs.s3a.server-side-encryption-algorithm' : 'SSE-KMS',
                'fs.s3.enableServerSideEncryption'        : 'true',
                'fs.s3.enableServerSideEncryption'        : 'true',
                'fs.s3.impl'                              : 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'fs.s3a.impl'                             : 'org.apache.hadoop.fs.s3a.S3AFileSystem',
                'fs.s3a.endpoint'                         : "s3.%s.amazonaws.com" % (jobConf['appconfs']['appdefaults'] or 'us-east-1')
            })

        
        spark = SparkSession \
                .builder \
                .config(conf=conf) \
                .appName(jobConf['name'] or 'PySparkApp') \
                .enableHiveSupport() \
                .getOrCreate()

        sc = spark.sparkContext        
        sc.setLogLevel(jobConf['appconfs']['logging']['sparkloglevel'] or 'INFO')
        if jobConf['appconfs']['logging']['sparkloglevel'] or 'INFO' == "DEBUG":
            msg = ""
            for k in sc._conf.getAll():
                msg += "\t%50s -> %s\n" % (k[0], k[1])
            log.debug("Initiated SparkSesion with below confs,\n{}".format(msg))
            
        return spark

    def sql(self, dfName : str, query : str,  partitions : int=0, persistType : str = None):
        '''
        Runs the inpult SQL, partitions the resulting Dataframe and persists the Dataframe if needed.

        Supported persistType: In addition to the pySpark native persist types, this function supports
        HIVE, HDFS, S3

        '''
        if persistType == None :
            _df = self.__spark.sql(self.handleHints(query = query))
            if partitions == 0 :
                df = _df
            elif df.rdd.getNumPartitions < partitions :
                df = _df.repartition(partitions)
            else:
                df = _df.coalesce(partitions)
            return df
        else:
            df = self.storeDF(
                df            = self.sql(query,partitions),
                dfName        = dfName,
                persistType   = persistType,
                partitions    = partitions,
                partitionCols = self.getPartitionColumnsFromSQL(query)
            )

        if dfName:
            df.createOrReplaceTempView(dfName)

        if self.__printcount:
            self.log("Number of Records in DF '%s' : %d " % (dfName,df.count()))

    def storeDF(self, df : DataFrame, dfName: str, persistType : str, partitions : int, partitionCols : list[str]):
        '''
        Store the input dataframe, read the persisted datafrme and return the new one.
        If Memory/Disk persistance requested, we run take(1) on the datafrme to force persist.
        '''
        persistTyp = persistType.toUpperCase()
        if self.__explainDF and \
            ( persistTyp not in ['NULL' , 'HIVE']) :
            self.log("\n\n\n")
            self.log("Execution plan for building the DF '%s' is," % (dfName))
            df.explain()
            self.log("\n\n\n")

        saveTyp = self.__saveDF if self.__saveDF and persistTyp not in ['NULL' , 'HIVE'] \
                  else persistTyp

        df1 = df if saveTyp not in ["HDFS","HIVE","S3" ] \
                 else self.repartitionDF(dataFrame= df, partitions = partitions)

        if  saveTyp in ["NULL" ,"NONE"]:
            return df1
        elif saveTyp == "HDFS":
            return self.persistExternal(parentDirURI= self.__tempHDFS, fileName= dfName, df= df, partitionCols= partitionCols)
        elif saveTyp == "S3":
            return self.persistExternal(parentDirURI= self.__tempS3,   fileName= dfName, df= df, partitionCols= partitionCols)
        elif saveTyp == "HIVE":
            return self.save2Hive(db = self.workDB, table=dfName, df=df,partitionCols=partitionCols)             
        elif saveTyp == "CHECK_POINT":
            return df.cache().checkpoint(eager=True)
        else:
            return self.persistLocal(dfName=dfName, df=df, persistType=persistType)            

    def persistLocal(self, dfName: str, df : DataFrame, persistType : str):
        ''' Persist the input Datafrmae locally (memory/disk/none) and runs `df.take(1)` to force persist.
        '''
        df.persist(self.getSparkPersistType(persistTypStr=persistType.toUpperCase()))

        if (self.__printcount == None) :
            row = df.take(1)
        return df

    def persistExternal(self, parentDirURI : str, fileName: str, df : DataFrame, partitionCols : list[str] = None, overwrite: bool = True ):
        '''
        Persist the input Dataframe to the external File storage.
        '''
        fullPath = \
               "%s%s" % (parentDirURI,fileName or "").replace("//", "/") \
                if parentDirURI.endswith("/") \
                else \
                "%s/%s" % (parentDirURI,fileName or "").replace("//", "/")
        schma = df.schema()
        write2ExtrFile(self.__fileFmt, path=fullPath, fileName=fileName,df=df, partitionCols=partitionCols,overwrite=overwrite)

        if parentDirURI.startswith("s3://") :
            pass

            #TODO:Yet to Implement
        df.unpersist() 
        if self.__fileFmt == 'parquet' :
            return self.readParquet(fullPath=fullPath, schma=schma)
        elif self.__fileFmt == 'orc' :
            return self.readOrc(fullPath=fullPath, schma=schma)
        elif self.__fileFmt == 'orc' :
            return self.readOrc(fullPath=fullPath, schma=schma)
        elif self.__fileFmt == 'csv' :
            return self.readCSV(fullPath=fullPath, schma=schma)
        elif self.__fileFmt == 'avro' :
            return self.readAvro(fullPath=fullPath, schma=schma)
        else :
            return self.readParquet(fullPath=fullPath, schma=schma)
        
    def write2ExtrFile(self,
                       flFmt         : str,
                       path          : str,
                       fileName      : str,
                       df            : DataFrame,
                       partitionCols : list[str] = None,
                       overwrite     : bool      = True ):
        '''
        Write the input DF to relevent external path 
        '''
        dfWrtr = None 
        if partitionCols: 
            log.info("Persisting DF to {0} with partition:{1}".format(path,",".join(partitionCols)))
            dfWrtr =  df.write \
                      .mode("overwrite" if overwrite else "append") \
                      .partitionBy(*partitionCols)
        else:
            dfWrtr =  df.write \
                      .mode("overwrite" if overwrite else "append")

        if self.__fileFmt == 'parquet' :
            dfWrtr.parquet(path)
        elif self.__fileFmt == 'orc' :
            dfWrtr.orc(path)
        elif self.__fileFmt == 'csv' :
            dfWrtr.csv(path)
        elif self.__fileFmt == 'avro' :
            dfWrtr.avro(path)
        else :
            dfWrtr.parquet(path)

    def save2Hive(self, db: str, table: str, df : DataFrame, partitionCols : list[str] = None, overwrite: bool = True):        
        partitions = "partitioned by ( {} )".format(",".join(partitionCols))  if partitionCols else ""
        if overwrite:
            tblPath = self.getTablePath(db,table)
            self.__spark.sql("drop table {0}.{1}".format(db,table))
            self.__hdfs.delDirs(tblPath)

           
    def getTablePath(self, db:str, table:str) -> str : 
        self.__spark.sql("""describe formatted {db}.{table}""") \
                    .filter("col_name = 'Location'") \
                    .select('data_type') \
                    .collect()[0]

    def getSparkPersistType(self, persistTypStr: str) -> StorageLevel :
        '''
            Converts the String representation to the StorageLevel Object.
            If invalid string received, it will return the `StorageLevel.NONE`
            Supported,
                `StorageLevel.NONE`
                `StorageLevel.DISK_ONLY`
                `StorageLevel.DISK_ONLY_2`
                `StorageLevel.MEMORY_ONLY`
                `StorageLevel.MEMORY_ONLY_2`
                `StorageLevel.MEMORY_AND_DISK`
                `StorageLevel.MEMORY_AND_DISK_2`
                `StorageLevel.OFF_HEAP`
        '''

        if   persistTypStr == "NONE"              : return StorageLevel.NONE
        elif persistTypStr == "DISK_ONLY"         : return StorageLevel.DISK_ONLY
        elif persistTypStr == "DISK_ONLY_2"       : return StorageLevel.DISK_ONLY_2
        elif persistTypStr == "MEMORY_ONLY"       : return StorageLevel.MEMORY_ONLY
        elif persistTypStr == "MEMORY_ONLY_2"     : return StorageLevel.MEMORY_ONLY_2
        elif persistTypStr == "MEMORY_AND_DISK"   : return StorageLevel.MEMORY_AND_DISK
        elif persistTypStr == "MEMORY_AND_DISK_2" : return StorageLevel.MEMORY_AND_DISK_2
        elif persistTypStr == "OFF_HEAP"          : return StorageLevel.OFF_HEAP
        else:
            self.log.WARN("Invalid Persist Type %s received. Defaulting to NONE" % (persistTypStr))
            return StorageLevel.NONE

    def repartitionDF(self,df : DataFrame, partitions : int = 0):
        '''
            Repartition the inuput dataframe

            parms: df          -> dataframe
                   partitions  -> new partitions count. Defaulted to 0 i.e Don't partition

            logic,
                if partitions = 0 , Don't repartitions
                if partitions = -1, Repartions to the default number (NumOfExecutors * ExecutorCores * 2)
                if partitions > 0 , Repartition/coalesce to the input number
        '''
        curParts = df.rdd.getNumPartitions
        finalParts = min(curParts, partitions)

        if curParts == partitions or partitions == 0:
            finalParts = -1
        elif partitions == -1:
            finalParts = self.__dfltRDDParts
        elif partitions > 0:
            finalParts = partitions
        else :
            pass #finalParts is pre-populated.

        self.log("Current Partitions: %d , Requested: %d,  Final: %d " % (curParts,partitions,finalParts) )

        if finalParts != -1:
            return  df
        elif curParts > finalParts:
            return df.coalesce(finalParts)
        else :
            return df.repartition(finalParts)


    def handleHints(self,query : str):
        '''
            Removes the SparkSQL hints if the -useHist parm is not set.

            Example:- If sql = 'select /* hists */ cols.. from ..'
               if -useHist is not set,
                  return 'select cols.. from ..'
               else
                  return 'select /* hists */ cols.. from ..'
        '''
        if self.__useHist:
            return query
        else:
            return  re.sub(r'/\*+.*\*/', '', query)

    @staticmethod
    def getPartitionColumnsFromSQL(query):
        s = query.toLowerCase().strip().replace("\n", " ")
        inx = s.index(" cluster ")
        lst = []
        if inx > 0:
            lst.extend((map(lambda x: x.strip(), s[inx + 12 :].split(","))))
        else :
            frm = s.index(" distribute ")
            to = s.index(" sort ", frm + 15) if frm > 0 else 0
            if to > frm:
                lst.extend((map(lambda x: x.strip(), s[frm + 15: to ].split(","))))
            else:
                lst.extend((map(lambda x: x.strip(), s[frm + 15:].split(","))))
        return lst
