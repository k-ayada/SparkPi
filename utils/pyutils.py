import sys,logging

class PyUtils:
    def __init__(self,argv=sys.argv, logLvl = "e",rootLogger= logging.getLogger(__name__)):
        self.lgr    = rootLogger
        self.parms  = PyUtils.argsToDict(argv)
        self.logLvl =  self.parms.get("--logLvl",logLvl)

        self.__setupLogger()
        self.lgr.info("Running jobs with Parameters,")
        for i,k in enumerate(self.parms):
            self.lgr.info("\t%02d. %-12s -> '%s'"%(i+1,"'"+k+"'", self.parms[k]))

    def getParms(self):
        return self.parms

    def getLogger(self):
        return self.lgr

    def __setupLogger(self, lvl :str = None):
        handler   = logging.StreamHandler(sys.stdout)
        dfmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(module)s.%(funcName)s[%(lineno)03d] %(message)s',dfmt)
        handler.setFormatter(formatter)
        self.logLvl = lvl or self.logLvl
        if self.logLvl == 'i':
            self.lgr.setLevel(logging.INFO)
        elif self.logLvl == 'w':
            self.lgr.setLevel(logging.WARN)
        elif self.logLvl == 'd':
            self.lgr.setLevel(logging.DEBUG)
        elif self.logLvl == 'c':
            self.lgr.setLevel(logging.CRITICAL)
        else :
            self.lgr.setLevel(logging.ERROR)

        self.lgr.addHandler(handler)

    @staticmethod
    def argsToDict(argv):
        parms = {}
        i = 1
        while i <= len(argv) -1:
            if ("--" == argv[i][0:2]):
                parms[argv[i]] = argv[i+1]
                i = i + 1
            else:
                parms[argv[i]] = argv[i]
            i = i + 1
        return parms
