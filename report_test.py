#! /usr/bin/python
from __future__ import print_function
### Original Module Imports
import psycopg2, time, datetime, smtplib, getpass, os, sys, glob, shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

### Added Module Imports for Gmail API interface
import httplib2
import os

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

import base64
###Modules already loaded.
#from email.mime.text import MIMEText
#from email.mime.multipart import MIMEMultipart


CONFIG_FILE = "reptest.config"

def load_config(config_file=CONFIG_FILE):
    """
    load_config checks <config_file> and loads variable declarations as a dictionary
    of "{VAR_NAME : VAR_VALUE}" construction
    """

    with open(config_file,"r") as c_file:
        config_out = c_file.read()

        config_out = config_out.split("\n")

        config = {}
        for i in range(len(config_out)):
            config_out[i] = config_out[i].replace(" ","",2)
            config[config_out[i][:config_out[i].find("=")]] = config_out[i][config_out[i].find("=")+1:]

        return config

try:
    config = load_config()
except:
    print("Check Config File...")
    sys.exit(1)

### Original Config Variables
dT = datetime.timedelta(0,int(config['dT']))
WORK_DIR = config['WORK_DIR'].replace("'","").replace('"',"")
LOG_SUB = config['LOG_SUB'].replace("'","").replace('"',"")
LOG_DIR = WORK_DIR + LOG_SUB
LOG_NAME = config['LOG_NAME'].replace("'","").replace('"',"")
DATABASE = config['DATABASE'].replace("'","").replace('"',"")
USER = config['USER'].replace("'","").replace('"',"")
HOST = config['HOST'].replace("'","").replace('"',"")
PASSWORD = config['PASSWORD'].replace("'","").replace('"',"")
QUERY_SUBJECT = config['QUERY_SUBJECT'].replace("'","").replace('"',"")
QUERY_PREDICATE = config['QUERY_PREDICATE'].replace("'","").replace('"',"")
#TO_EMAIL = config['TO_EMAIL'].replace("'","").replace('"',"")
#FROM_EMAIL = config['FROM_EMAIL'].replace("'","").replace('"',"")
#SEND_EMAIL_USER = config['SEND_EMAIL_USER'].replace("'","").replace('"',"")
#SEND_EMAIL_PASSWORD = config['SEND_EMAIL_PASSWORD'].replace("'","").replace('"',"")
#SUBJECT_NAME = config['SUBJECT_NAME'].replace("'","").replace('"',"")
LOG_HEADER = config['LOG_HEADER'].replace("'","").replace('"',"")

### Newly Added Config Variables for Gmail Integration
HOME_DIR = os.path.expanduser('~')
SCOPES = config['SCOPES'].replace("'","").replace('"',"")
CLIENT_SECRET_FILE = config['CLIENT_SECRET_FILE'].replace("'","").replace('"',"")
APPLICATION_NAME = config['APPLICATION_NAME'].replace("'","").replace('"',"")
CREDENTIAL_NAME = config['CREDENTIAL_NAME'].replace("'","").replace('"',"")
EMAIL_BODY = config['EMAIL_BODY'].replace("'","").replace('"',"")
TO_EMAIL = config['TO_EMAIL'].replace("'","").replace('"',"")
FROM_EMAIL = config['FROM_EMAIL'].replace("'","").replace('"',"")
EMAIL_SUBJECT = config['EMAIL_SUBJECT'].replace("'","").replace('"',"")

### New Function Declarations for Gmail API
def get_credentials(home_dir=HOME_DIR,credential_name=CREDENTIAL_NAME,application_name=APPLICATION_NAME,client_secret_file=CLIENT_SECRET_FILE,scopes=SCOPES):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    credential_dir = os.path.join(home_dir, '.credentials')

    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   credential_name)

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret_file, scopes)
        flow.user_agent = application_name
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_service(credentials=get_credentials()):
    """
    Returns Service To use in Gmail access
    """
    print("Getting Service")
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)
    return service

def initial_message(to_email=TO_EMAIL,from_email=FROM_EMAIL,email_subject=EMAIL_SUBJECT,email_body=EMAIL_BODY):
    """
    Creates initial email skeleton
    """
    msg = MIMEMultipart()
    msg['to'] = to_email
    msg['from'] = from_email
    msg['subject'] = email_subject
    msg.attach(MIMEText(email_body))
    return msg

def attach_textfile(file_path,message=initial_message()):
    """
    Attaches a file to an existing message
    """
    #file_path = file_dir + "/" + file_name
    print("\tOpening " + file_path + ".\n\tCreating Attachment")
    with open(file_path,'rb') as fp:
        msg = MIMEText(fp.read())
    print("\tAttaching Header")
    msg.add_header('Content-Disposition','attachment',filename=file_name)
    print("\tAttaching File to " + str(msg))
    message.attach(msg)
    #return {'raw': base64.urlsafe_b64encode(message.as_string())}
    return message

def send_email(message,service,user_id='me'):
    """Send email message
    Args:
        service: authorized gmail api service instance.
        user_id: user's email address. 'me' indicates authenticated user.
        message: message to be sendin'
    """
    message = {'raw': base64.urlsafe_b64encode(message.as_string())}
    try:
        print('\tTrying To Send Message...')
        message = (service.users().messages().send(userId=user_id,body=message).execute())
        print('\tMessage Sent')
        #print('Message Subject: %s' % message['subject'])
        return message
    except:
        print('\tAn error occurred...')



### Original Function Declarations

## Time Logging Functions

def write_log(work_dir=WORK_DIR):
    """
    write_log logs current time as 'Last Run Time'
    """
    TimeLog = open(work_dir + "/time.log","w")
    T_N = datetime.datetime.now()
    TimeLog.write(str(T_N))
    TimeLog.close()


def read_log(work_dir=WORK_DIR):
    """
    read_log retrieves saved 'Last Run Time'
    """
    try:
        TimeLog = open(work_dir + "/time.log","r")
        T_LR = TimeLog.read()
        T_LR = datetime.datetime.strptime(T_LR[0:19], '%Y-%m-%d %H:%M:%S')
        TimeLog.close()
        if T_LR == None or len(str(T_LR)) < 19:
            print("WARNING! Time Last Run not found. Please Run Manual Reports")
            T_N = datetime.datetime.now()
            T_LR = datetime.datetime(T_N.year,T_N.month,T_N.day)
            print("Last Run Time set to: %s" % str(T_LR))
    except:
        print("WARNING! Time Last Run not found. Please Run Manual Reports")
        TimeLog = open(work_dir + "/time.log","w")
        T_N = datetime.datetime.now()
        T_LR = datetime.datetime(T_N.year, T_N.month, T_N.day)
        TimeLog.write(str(T_LR))
        print("""Last Run Time set to: %s""" % str(T_LR))
        TimeLog.close()
    return T_LR


## Internal Time Functions
#def set_tnr(now=1, T_LR):
def set_tnr(T_LR, now=1, dT=dT):
    """
    set_tnr sets next run time
    """
    if now == 1:
        T_NR = datetime.datetime.now() + dT
    else:
        T_NR = T_LR + dT
    return T_NR


## Database Functions
def run_query(T_LR, db, usr, hst, pswd,query_subject=QUERY_SUBJECT,query_predicate=QUERY_PREDICATE):
    """
    run_query returns the results of the report query
    """
    q_var = """ > '%s'""" % T_LR
    query = query_subject + q_var + query_predicate
    conn = psycopg2.connect(database=db, user=usr, host=hst, password=pswd)
    cursor = conn.cursor()
    cursor.execute(query)
    q_result = cursor.fetchall()
    conn.commit()
    conn.close()
    return q_result

### Newly Added for Gmail API Integration

def package_email(log_dir=LOG_DIR, log_name=LOG_NAME):
    """
    Builds on Gmail API functions to produce Email to send.

    Args:
        log_dir: directory in which logs are stored
        log_name: name pattern to glob for attachment

    Returns:
        message: the message with attachments to send.

    """
    print("\tChanging Directory to: " + log_dir)
    os.chdir(log_dir)
    print("\tGathering file list")
    file_list = glob.glob("*"+log_name)
    print("\tBuilding Message Skeleton")
    message = initial_message()
    print("\tAttaching Files")
    if len(file_list) > 0:
        for i in file_list:
            try:
                message = attach_textfile(log_dir + "/" + i, message)
            except:
                print("File: " + i + " failed to attach")
    print("\tFiles Attached")
    return message
    #return {'raw': base64.urlsafe_b64encode(message.as_string())}


### original send_results is being deprecated by Gmail API Method.
#def send_results(T_LR, log_dir=LOG_DIR, to_email=TO_EMAIL, from_email=FROM_EMAIL, send_email_user=SEND_EMAIL_USER, send_email_password=SEND_EMAIL_PASSWORD, log_name=LOG_NAME, subject_name=SUBJECT_NAME):
#    """
#    SMTP Negotiation and sending (T_LR is used to attach a unique identifier as the subject)
#    """
#    sent_logs = log_dir + "/sent"
#    msg = MIMEMultipart()
#    msg['From'] = from_email
#    msg['To'] = to_email
#    msg['Subject'] = "%s: %s" % (subject_name, str(T_LR)[:19])
#    os.chdir(log_dir)
#    filenames = glob.glob("*"+log_name)
#    for i in filenames:
#        with file(i) as fp:
#            attachment = MIMEText(fp.read())
#            attachment.add_header('Content-Disposition','attachment',filename=i)
#            msg.attach(attachment)
#    if len(filenames) > 0:
#        s = smtplib.SMTP("smtp.gmail.com",587)
#        s.ehlo()
#        s.starttls()
#        s.ehlo()
#        s.login(send_email_user,send_email_password)
#        s.sendmail(msg['From'],msg['To'].split(","),msg.as_string())
#        s.close()


## Miscellaneous Service Scripts
def sav_res(T_LR,res,log_dir=LOG_DIR,log_name=LOG_NAME, log_header=LOG_HEADER):
    """
    sav_res() commits query results to a log file
     of the following name : "YYYY-MM-DD HH:MM:SS - <log_name>" in ./logs
    """
    if len(res) > 0:
        #Fix Tuples to Assemble output file
        for i in range(len(res)):
            try:
                res[i] = list(res[i])
            except:
                continue
        #Define file name, header
        ### REFACTOR header
        out_name = log_dir + "/" + str(T_LR)[:19].replace(":","") + " - " + log_name
        #Open File, Insert Results
        with open(out_name,"w") as fp:
            fp.write(log_header)
            for i in res:
                s = "%s,'%s',\n" % (i[0],i[1])
                fp.write(s)
    # REFACTOR THIS. if len(res) < 0 you don't get any useful information.

# Log Management (put away files already sent)
def mk_sent(log_dir=LOG_DIR,log_name=LOG_NAME):
    ## REFACTOR THIS... USE GLOBALS
    sent_logs = log_dir + "/sent"
    os.chdir(log_dir)
    f = glob.glob("*" + log_name)
    for i in f:
        shutil.move(i,sent_logs)


#### Here starts the action.

### REFACTOR THIS.... if __name__ == '__main__': like the other file.
def main():
    # Get T_LR from log if exists
    T_LR = read_log()
    # Initialize Next Run based on last-run.
    T_NR = set_tnr(T_LR,0)

    #### START MAIN LOOP

    ###Refactoring largely done.
    TTerminal = datetime.datetime.now() + datetime.timedelta(0,600)
    while datetime.datetime.now() < TTerminal:
        #Get query results
        print("running the query")
        res = run_query(T_LR, DATABASE, USER, HOST, PASSWORD)
        #Save T_LR in log
        print("writing logs")
        write_log()
        #Set new T_NR
        print("set new T_NR")
        T_NR = set_tnr(T_LR)
        #Refresh T_LR from log
        print("refresh T_LR")
        T_LR = read_log()
        #Save Results
        print("sav_res")
        sav_res(T_LR,res)
        ### Send results via email
        #print "trying to send"
        ### Deprecated Method Being Replaced...
        try:
            #print("Not actually sending results...")
            print("Packaging Email")
            message = package_email()
            print("Getting Service")
            service = get_service()
            print("Sending Email")
            send_email(message, service)
            #send_results(T_LR)
            #print "Not Marking Sent"
            print("mk_sent")
            mk_sent()
        except:
            print("Send Failed")
            continue
        print("Waiting " + str(dT.total_seconds()) + "s")
        while datetime.datetime.now() < T_NR:
            time.sleep(5)

if __name__ == '__main__':
    main()
