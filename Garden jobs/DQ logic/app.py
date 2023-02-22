import paramiko
import boto3
import io
from avant_python_utils import email
import os
from datetime import datetime
import trellis
import time

# Extracting run_id
def generate_run_id(run_id):
    run_id = run_id.replace("etl-", "")
    run_id = run_id.replace("for-", "")
    run_id = run_id.replace("webbank-", "")
    run_id = run_id.replace("file-", "")
    run_id = run_id.replace("tranfer-", "")
    return run_id


email_list = ['ethan.feldman@avant.com']
run_id = generate_run_id(os.getenv('GARDEN_ID'))
print(run_id)



def open_ftp_connection(ftp_host, ftp_username, ftp_key): 
    client = paramiko.SSHClient()
    client.load_system_host_keys() 
    try: 
        transport = paramiko.Transport(ftp_host) 
    except Exception as e: 
        return 'conn_error' 
    try: 
        transport.connect(username=ftp_username,pkey=ftp_key)
    except Exception as identifier: 
        return 'auth_error' 
    ftp_connection = paramiko.SFTPClient.from_transport(transport) 
    return ftp_connection 

def transfer_file_from_ftp_to_s3(bucketname, ftp_file_path, s3_file_path):
    sftp_k = trellis.keys("web_bank_pos_sftp")
    ftp_connection = open_ftp_connection(
        sftp_k["host"]
        , sftp_k["user"]
        , paramiko.RSAKey.from_private_key(io.StringIO(sftp_k["private_key"]))
    )
    ftp_file = ftp_connection.file(ftp_file_path, 'r') 
    k = trellis.keys("webbank.aws")
    s3_connection = boto3.client('s3',aws_access_key_id=k["access_key"],aws_secret_access_key=k["secret_key"]) 
    print("Transferring complete File from FTP to S3...")
    ftp_file_data = ftp_file.read()
    ftp_file_data_bytes = io.BytesIO(ftp_file_data)
    s3_connection.upload_fileobj(ftp_file_data_bytes, bucketname, s3_file_path)
    print("Successfully Transferred file from FTP to S3!")
    ftp_file.close()

# Establishing sftp connection
sftp_k = trellis.keys("web_bank_pos_sftp")
ftp_connection = open_ftp_connection(
        sftp_k["host"]
        , sftp_k["user"]
        , paramiko.RSAKey.from_private_key(io.StringIO(sftp_k["private_key"]))
    )

# set base directory
directory_structure = ftp_connection.listdir()
print("Viewing objects in the directory")
print(directory_structure)

if __name__ == '__main__':
    trellis.start()
    
    # view subdirectories
    for sub in ftp_connection.listdir('FromWB'):
        print("--------------------------")
        print("Looking inside of:", sub)
        print("--------------------------")
        # view directory objects
        
        if sub == 'archive':
            pass
        else:
            for attr in ftp_connection.listdir('FromWB/'+sub):
                print("--------------------------")
                print("Processing file:", attr)
                print("--------------------------")

            # Detect file AvantPOSDDMMA.ACH and send email     
                print("Searching for the AvantPOSDDMMA.ACH file")
                if (attr[-4:]=='.ACH'):
                    print("Found AvantPOSDDMMA.ACH file")

                    email.send_email(
                                  to=email_list
                                , subject='WebBank Has Placed a Failed Funding NACHA in SFTP'
                                , credentials=trellis.keys('automate_email')
                                , text='The file AvantPOSDDMMA.ACH has appeared in the current run for the {} product. The file can be found in the output section of this link https://garden.services.global.avant.com/runs/'.format(sub)+run_id
                                , explicit_to=True)
                    trellis.output("ACH_File", attr)
                    ftp_connection.remove("FromWB/"+attr)


                else:
                    print("Nacha file not found")

            #  detect _sale_file.csv files
                print("--------------------------")
                print("Searching for the sale file")
                if (attr[-14:]=='_sale_file.csv'):
                    print("Found Sale file for the {} product".format(sub))
                    ftp_file_path= 'FromWB/' + sub + '/' + attr

                    if ftp_connection == "conn_error":
                        print("Failed to connect FTP Server!")
                    elif ftp_connection == "auth_error":
                        print("Incorrect username or password!")
                    else:
                        try:
                            ftp_file = ftp_connection.file(ftp_file_path,"r")
                        except Exception as e:
                            print("File does not exists on FTP Server inside {}!".format(sub))
                        transfer_file_from_ftp_to_s3(
                                bucketname= 'avant-partner01-landing-non-prod',
                                ftp_file_path = ftp_file_path,
                                s3_file_path = 'pos/webbank/'+ sub + '/' +attr
                    )
#                         transfer_file_from_ftp_to_s3(
#                                 bucketname= 'avant-partner01-landing',
#                                 ftp_file_path = ftp_file_path,
#                                 s3_file_path = 'pos/webbank/'+ sub + '/' +attr
#                     )

                    # Moving the sale file to archive if it doesn't exists in the archive with timestamp
                    ftp_connection.rename('FromWB/' + sub + '/' + attr, 'FromWB/archive/' + sub + '/' + time.strftime("%H%M%S")+"_"+ attr)
                else:
                    print("Sales file not found")

    ftp_connection.close()
    trellis.finish()
