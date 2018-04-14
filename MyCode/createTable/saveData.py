# encoding = utf-8
import MySQLdb


def create_basic_table():
    '''
    create a basic table in which records the basic characters
    :return:
    '''
    create_sql = """create table basic_table(Event_type VARCHAR (100), Version_Number VARCHAR(100), Event_Time INT,jobId INT, userId INT,
                  options INT , numProcessors INT , submitTime INT , beginTime INT , termTime INT , startTime INT,
                  userName VARCHAR(100),queue VARCHAR(100), resReq VARCHAR(100), dependCond VARCHAR(100),
                  preExecCmd VARCHAR(100), fromHost VARCHAR(100),
                  cwd TEXT, inFileName VARCHAR(100),outFileName VARCHAR(100), errFileName VARCHAR(100),
                  jobFileName VARCHAR(100),numAskedHosts INT,
                  AskedHostsList VARCHAR(2000), numExhosts INT, ExhostsList VARCHAR(2000),
                  jStatus INT, hostFactor FLOAT, jobName VARCHAR(100), command TEXT, ru_utime FLOAT,
                  ru_stime FLOAT, ru_maxrss FLOAT, ru_ixrss FLOAT, ru_ismrss FLOAT, ru_idrss FLOAT, ru_isrss FLOAT,
                  ru_minflt FLOAT, ru_majflt FLOAT, ru_nswap FLOAT, ru_inblock FLOAT, ru_oublock FLOAT,
                  ru_ioch FLOAT, ru_msgsnd float, ru_msgrcv FLOAT, ru_nsignals FLOAT, ru_nvcsw FLOAT,
                  ru_nivcsw FLOAT, ru_excutime FLOAT, mailUser VARCHAR(100), projectName varchar(100),
                  exitStatus INT, maxNumProcessors INT, loginShell VARCHAR(100), timeEvent VARCHAR(100),
                  idx INT , maxRMem INT, maxRSwap INT, inFileSpool VARCHAR(100), commandSpool VARCHAR(100),
                  rsvId VARCHAR(100), sla VARCHAR(100), exceptMask INT, additionalInfo VARCHAR(100),
                  exitInfo INT, warningAction VARCHAR(100), warningTimePeriod INT, chargedSAAP VARCHAR(100),
                  licenseProject VARCHAR(100), app VARCHAR(100), postExecCmd VARCHAR(100), runTimeEstimation INT,
                  jobGroupName VARCHAR(100), requeueEvalues VARCHAR(100), option2 INT, resizeNotifyCmd varchar(100),
                  lastResizeTime INT, rsvId2 VARCHAR(100), jobDescription VARCHAR(100), submitEXTNum INT,
                  submitEXTKey VARCHAR(100), submitEXTValue VARCHAR(100), numHostRusage INT)
                 """
    cursor.execute(create_sql)


def gen(arr):
    '''
    A generator for each property
    :param arr:
    :return:
    '''
    # try:
    #     for i in range(20000):
    #         yield arr[i]
    # except:
    #     print "error raise"
    for i in range(20000):
        yield arr[i]

def getstat(jobs, log, prop, prop_type):
    '''
    Get stat "prop" for job
    :param jobs:
    :param log: the raw value list
    :param prop: character name
    :param prop_type: character type
    :return:
    '''
    value = log.next()

    # handle string: legal string starts and ends with '"'
    if prop_type == str:
        if value[0] == '"':
            if len(value) < 2:
                value += log.next()
            while not (value[:] == '""' or value[-1] == '"' and value[-2] != '"'
                       or value[-3:] == '"""'and value[-4] != '"'):
                value = value + ' ' + log.next()
            value = value[1:-1]


    # handle integer
    elif prop_type == int:
        value = int(value)

    # handle float
    elif prop_type == float:
        value = float(value)

    else:
        raise Exception
    jobs[prop] = value
    # print("prop", prop, "value", value)
    return value


def get_job_value(log_line):
    '''
    get all character value of one job
    :param log_line:
    :return job:
    '''
    job = {}
    getstat(job, log_line, 'Event_type', str)
    getstat(job, log_line, 'Version_Number', str)
    getstat(job, log_line, 'Event_Time', int)
    getstat(job, log_line, 'jobId', int)
    getstat(job, log_line, 'userId', int)
    getstat(job, log_line, 'options', int)
    getstat(job, log_line, 'numProcessors', int)
    getstat(job, log_line, 'submitTime', int)
    getstat(job, log_line, 'beginTime', int)
    getstat(job, log_line, 'termTime', int)
    getstat(job, log_line, 'startTime', int)
    getstat(job, log_line, 'userName', str)
    getstat(job, log_line, 'queue', str)
    getstat(job, log_line, 'resReq', str)
    getstat(job, log_line, 'dependCond', str)
    getstat(job, log_line, 'preExecCmd', str)
    getstat(job, log_line, 'fromHost', str)
    getstat(job, log_line, 'cwd', str)
    getstat(job, log_line, 'inFileName', str)
    getstat(job, log_line, 'outFileName', str)
    getstat(job, log_line, 'errFileName', str)
    getstat(job, log_line, 'jobFileName', str)

    getstat(job, log_line, 'numAskedHosts', int)
    # using numAskedHosts
    # AskedHostsNames = ''
    # for i in range(job['numAskedHosts']):
    #     AskedHostsNames += getstat(job, log_line, 'askedHosts' + str(i), str).strip("node")
    #     AskedHostsNames += " "
    # job["AskedHostsList"] = AskedHostsNames
    askedhostsName = {}
    for each in range(job['numAskedHosts']):
        tempName = getstat(job, log_line, 'askedHosts' + str(each), str)
        if tempName not in askedhostsName.keys():
            askedhostsName[tempName] = 1
        else:
            askedhostsName[tempName] += 1
    askedhostsLists = ''
    for key in askedhostsName.keys():
        keyNum = askedhostsName[key]
        keyName = key.strip('node')
        keyNew = keyName + "_" + str(keyNum)
        askedhostsLists += keyNew
        askedhostsLists += ' '
    job['AskedHostsList'] = askedhostsLists
    getstat(job, log_line, 'num_exceptHosts', int)
    # using numExhosts
    # ExHostsName = ''
    # for i in range(job['num_exceptHosts']):
    #     ExHostsName += getstat(job, log_line, 'execHosts' + str(i), str).strip("node")
    #     ExHostsName += " "

    # job['ExhostsList'] = ExHostsName
    exhostsName = {}
    for each in range(job['num_exceptHosts']):
        tempName = getstat(job, log_line, 'execHosts' + str(each), str)
        if tempName not in exhostsName.keys():
            exhostsName[tempName] = 1
        else:
            exhostsName[tempName] += 1
    exhostsLists = ''
    for key in exhostsName.keys():
        keyNum = exhostsName[key]
        keyName = key.strip('node')
        keyNew = keyName+"_"+str(keyNum)
        exhostsLists += keyNew
        exhostsLists += ' '
    job['ExhostsList'] = exhostsLists
    getstat(job, log_line, 'jStatus', int)
    getstat(job, log_line, 'hostFactor', float)
    getstat(job, log_line, 'jobName', str)
    getstat(job, log_line, 'command', str)
    getstat(job, log_line, 'ru_utime', float)
    getstat(job, log_line, 'ru_stime', float)
    getstat(job, log_line, 'ru_maxrss', float)
    getstat(job, log_line, 'ru_ixrss', float)
    getstat(job, log_line, 'ru_ismrss', float)
    getstat(job, log_line, 'ru_idrss', float)
    getstat(job, log_line, 'ru_isrss', float)
    getstat(job, log_line, 'ru_minflt', float)
    getstat(job, log_line, 'ru_majflt', float)
    getstat(job, log_line, 'ru_nswap', float)
    getstat(job, log_line, 'ru_inblock', float)
    getstat(job, log_line, 'ru_oublock', float)
    getstat(job, log_line, 'ru_ioch', float)
    getstat(job, log_line, 'ru_msgsnd', float)
    getstat(job, log_line, 'ru_msgrcv', float)
    getstat(job, log_line, 'ru_nsignals', float)
    getstat(job, log_line, 'ru_nvcsw', float)
    getstat(job, log_line, 'ru_nivcsw', float)
    getstat(job, log_line, 'ru_exutime', float)
    getstat(job, log_line, 'mailUser', str)
    getstat(job, log_line, 'projectName', str)
    getstat(job, log_line, 'exitStatus', int)
    getstat(job, log_line, 'maxNumProcessors', int)
    getstat(job, log_line, 'loginShell', str)
    getstat(job, log_line, 'timeEvent', str)
    getstat(job, log_line, 'idx', int)
    getstat(job, log_line, 'maxRMem', int)
    getstat(job, log_line, 'maxRSwap', int)
    getstat(job, log_line, 'inFileSpool', str)
    getstat(job, log_line, 'commandSpool', str)
    getstat(job, log_line, 'rsvId', str)
    getstat(job, log_line, 'sla', str)
    getstat(job, log_line, 'exceptMask', int)
    getstat(job, log_line, 'additionalInfo', str)
    getstat(job, log_line, 'exitInfo', int)
    getstat(job, log_line, 'warningAction', str)
    getstat(job, log_line, 'warningTimePeriod', int)
    getstat(job, log_line, 'chargedSAAP', str)
    getstat(job, log_line, 'licenseProject', str)
    getstat(job, log_line, 'app', str)
    getstat(job, log_line, 'postExecCmd', str)
    getstat(job, log_line, 'runTimeEstimation', int)
    getstat(job, log_line, 'jobGroupName', str)
    getstat(job, log_line, 'requeueEvalues', str)
    getstat(job, log_line, 'option2', int)
    getstat(job, log_line, 'resizeNotifyCmd', str)
    getstat(job, log_line, 'lastResizeTime', int)
    getstat(job, log_line, 'rsvId2', str)
    getstat(job, log_line, 'jobDescription', str)
    getstat(job, log_line, 'submitEXTNum', int)
    getstat(job, log_line, 'submitEXTKey', str)
    getstat(job, log_line, 'submitEXTValue', str)
    getstat(job, log_line, 'numHostRusage', int)
    return job

def insert_basic_table(newjob):
    '''
    insert the job value into the basic table
    :param newjob:
    :return:
    '''
    sql = "insert into basic_table(Event_type, Version_Number, Event_Time, jobId, userId, options, numProcessors," + \
          "submitTime, beginTime, termTime, startTime, userName, queue, resReq, dependCond, preExecCmd, fromHost," + \
          "cwd, inFileName, outFileName, errFileName, jobFileName, numAskedHosts, AskedHostsList, numExhosts, " \
          "ExhostsList, jStatus, hostFactor, jobName, command, ru_utime, ru_stime, ru_maxrss, ru_ixrss, ru_ismrss" \
          ", ru_idrss, ru_isrss, ru_minflt, ru_majflt, ru_nswap, ru_inblock, ru_oublock, ru_ioch, ru_msgsnd, " \
          "ru_msgrcv, ru_nsignals, ru_nvcsw, ru_nivcsw, ru_excutime, mailUser, projectName, exitStatus, " \
          "maxNumProcessors, loginShell, timeEvent, idx, maxRMem, maxRSwap, inFileSpool, commandSpool," \
          "rsvId, sla, exceptMask, additionalInfo, exitInfo, warningAction, warningTimePeriod, chargedSAAP, " \
          "licenseProject, app, postExecCmd, runTimeEstimation, jobGroupName, requeueEvalues, option2," \
          "resizeNotifyCmd, lastResizeTime, rsvId2, jobDescription, submitEXTNum, submitEXTKey, submitEXTValue," \
          "numHostRusage) VALUES ('%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s','%s','%s', '%s'," \
          " '%s', '%s', '%s', " \
          "'%s', '%s', '%s', '%s', %d, '%s', %d, '%s', %d, %f, '%s', '%s', %f, %f, %f, %f, %f, %f," \
          " %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, '%s', '%s', %d, %d, " \
          "'%s', '%s', %d, %d, %d, '%s', '%s', '%s', '%s', %d, '%s', %d, " \
          "'%s', %d, '%s', '%s', '%s', '%s', %d, '%s', '%s', %d, '%s', %d, '%s', '%s', %d, '%s', '%s', %d)" % (newjob['Event_type']
           , newjob['Version_Number'], newjob['Event_Time'], newjob['jobId'], newjob['userId']
           , newjob['options'], newjob['numProcessors'], newjob['submitTime'], newjob['beginTime']
           , newjob['termTime'], newjob['startTime'], newjob['userName'], newjob['queue']
           , newjob['resReq'], newjob['dependCond'], newjob['preExecCmd'], newjob['fromHost']
           , newjob['cwd'], newjob['inFileName'], newjob['outFileName'], newjob['errFileName']
           , newjob['jobFileName'], newjob['numAskedHosts'], newjob['AskedHostsList'], newjob['num_exceptHosts']
           , newjob['ExhostsList'], newjob['jStatus'], newjob['hostFactor'], newjob['jobName']
           , newjob['command'], newjob['ru_utime'], newjob['ru_stime'], newjob['ru_maxrss']
           , newjob['ru_ixrss'], newjob['ru_ismrss'], newjob['ru_idrss'], newjob['ru_isrss']
           , newjob['ru_minflt'], newjob['ru_majflt'], newjob['ru_nswap'], newjob['ru_inblock']
           , newjob['ru_oublock'], newjob['ru_ioch'], newjob['ru_msgsnd'], newjob['ru_msgrcv']
           , newjob['ru_nsignals'], newjob['ru_nvcsw'], newjob['ru_nivcsw'], newjob['ru_exutime']
           , newjob['mailUser'], newjob['projectName'], newjob['exitStatus'], newjob['maxNumProcessors']
           , newjob['loginShell'], newjob['timeEvent'], newjob['idx'], newjob['maxRMem']
           , newjob['maxRSwap'], newjob['inFileSpool'], newjob['commandSpool'], newjob['rsvId']
           , newjob['sla'], newjob['exceptMask'], newjob['additionalInfo'], newjob['exitInfo']
           , newjob['warningAction'], newjob['warningTimePeriod'], newjob['chargedSAAP'], newjob['licenseProject']
           , newjob['app'], newjob['postExecCmd'], newjob['runTimeEstimation'], newjob['jobGroupName']
           , newjob['requeueEvalues'], newjob['option2'], newjob['resizeNotifyCmd'], newjob['lastResizeTime']
           , newjob['rsvId2'], newjob['jobDescription'], newjob['submitEXTNum'], newjob['submitEXTKey']
           , newjob['submitEXTValue'], newjob['numHostRusage'])
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception, e:
        conn.rollback()
        print e
        raise e


if __name__ == "__main__":
    # connect to the database and get the cursor
    # pay attention to that you should set the charset to utf8
    # if there are chinese in the table

    conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="usermodel")
    cursor = conn.cursor()
    print "connet the usermodel database successfully"

    # # insert the basic table
    # this operation has already done
    # create_basic_table()

    # read the input file
    filename = raw_input('Input file name:\n')
    # like format like F:/usermodel/lsb/lsb.acct.20171127
    infile = open(filename, 'r')
    # record the job name
    counter = 0
    for line in infile:
        counter += 1
        if counter <= 381148:
            continue
        print "job No." + str(counter)
        try:
            print line
            log_line = gen(line.replace("'", '""').split())
            print log_line
            job = get_job_value(log_line)
            insert_basic_table(job)
            print "insert successfully!\n"
        except Exception, e:
            # print "job No." + str(counter)
            print "job illegal and the line is:"
            print line
            print e
    conn.close()

