
import hdfs

class Hdfs:
    def __init__(self, log : RootLogger, hadoop):

        self.__hadoop = hadoop
        self.__conf   = hadoop.conf.Configuration()
        self.__fs     = hadoop.fs.FileSystem
    def hdfsPathObj(self, path:str)  :
        return self.__hadoop.fs.Path(path)

    def fileExists(self, path:str) -> bool:
        fileToCheck = hdfsPathObj(path)
        self.__fs.exists(fileToCheck)
        
    def delDirs(self,path:str) -> bool:
        pth = hdfsPathObj(path)
        if self.__fs.exists(pth): 
            self.__fs.delete(pth, True)
        


