import json
import boto3
import socket
import time
import datetime
from datetime import datetime

backup = boto3.client('backup')
ec2 = boto3.client('ec2')
dynamodb = boto3.client('dynamodb')
volume_available_waiter = ec2.get_waiter('volume_available')

#Get the List of Volume IDs which Having DeleteOnTermination=False
#####################################################
def get_volume_id(instance_id):
    instance_details = ec2.describe_instances(
        InstanceIds=[
            instance_id
        ]
    )
    #Collect the EBS volume IDs ehich having DeleteOnTermination==false
    volume_id_list = []
    for r in instance_details['Reservations']:
        for i in r['Instances']:
            for b in i['BlockDeviceMappings']:
                if not b['Ebs']['DeleteOnTermination']:
                    volume_id_list.append(b['Ebs']['VolumeId'])
    return volume_id_list

#Detach & Delete Volumes on Specific Instance
#####################################################
def delete_volumes(volume_id_list, instance_id):
    if volume_id_list:
        for volume_id in volume_id_list:
            response = ec2.detach_volume(
                Force=True,
                InstanceId=instance_id,
                VolumeId=volume_id,
            )
            
        volume_available_waiter.wait(
            VolumeIds=volume_id_list
        )  
        for volume_id in volume_id_list:    
            #print(volume_id)
            delete_request = ec2.delete_volume(
                VolumeId=volume_id
            )
        print('Deleted all Volumes on RestoreTestInstance')
    else:
        print('No attachements to Delete on RestoreTestInstance')
    
#Terminate instances
#####################################################
def terminate_instance(instance_id):
    try:
        ec2 = boto3.resource('ec2')
        ec2.Instance(instance_id).modify_attribute(
            DisableApiTermination={
                'Value': False
            }
        )
        time.sleep(5)
        ec2.Instance(instance_id).terminate()
    except Exception as e:
        print(str(e))
        return
    
#Get Instance Name
#####################################################
def get_instanceName(instance_id):
    ec2r = boto3.resource('ec2')
    ec2instance = ec2r.Instance(instance_id)
    instance_name = ''
    for tags in ec2instance.tags:
        if tags["Key"] == 'Name':
            instance_name = tags["Value"]
            
    return instance_name

#Store data on DynamoDB
#####################################################
def put_itemDynamodb(backup_timestamp, original_instance_id, original_instance_name, job_id, job_type, job_status):
    dynamodb.put_item(TableName='AWSBackupStatus', Item={'TimeStamp':{'N':str(backup_timestamp)},'InstanceID':{'S':original_instance_id},'InstanceName':{'S':original_instance_name},'JobID':{'S':job_id},'JobTydpe':{'S':job_type},'JobStatus':{'S':job_status}})
    print('Stored Job Status on DynamoDB')

#####################################################
#Main lambda_handler
#####################################################
def lambda_handler(event, context):
    
    #Print SNS message if required
    #print('Incoming Event:' + json.dumps(event))
    
    #Get Data from SQS Body
    sqs_body = event['Records'][0]['body']
    event_object = json.loads(sqs_body)
    
    try:
        #If SNS subject "AWS Backup Restore Test", do nothing
        if 'AWS Backup Restore Test' in event_object['Subject']:
            print('No action required, deletion of new resource confirmed.')
            return
    except Exception as e:
            print(str(e))
            return
    
    #Get the Job type (backup/restore) from SNS message
    job_type = event_object['Message'].split('.')[-1].split(' ')[1]
    
    try:
        if 'failed' in event_object['Message']:
            print('Something has failed. Please review the job in the AWS Backup console.')
            return 'Job ID:' + event_object['Message'].split('.')[-1].split(':')[1].strip()
            
        elif job_type == 'Backup':
            
            backup_timestamp = int(datetime.timestamp(datetime.now()))

            #Get the Backup Job ID from SNS Message
            backup_job_id = event_object['Message'].split('.')[-1].split(':')[1].strip()
            
            #Getting the Original Instance Details
            original_instance_id = event_object['Message'].split('.')[-2].split(':')[6].split('/')[1].strip()
            original_instance_name = get_instanceName(original_instance_id)
            
            #Store the Status on DynamoDB
            #Uncomment the below line if you need to send the job status to DynamoDB
            #put_itemDynamodb(backup_timestamp, original_instance_id, original_instance_name, backup_job_id, job_type, "Success")
            
            #Get Backup Job more details
            backup_info = backup.describe_backup_job(
                BackupJobId=backup_job_id
            )
            
            recovery_point_arn = backup_info['RecoveryPointArn']
            iam_role_arn = backup_info['IamRoleArn']
            backup_vault_name = backup_info['BackupVaultName']
            resource_type = backup_info['ResourceType']

            #Get restore point restore metadata
            metadata = backup.get_recovery_point_restore_metadata(
                BackupVaultName=backup_vault_name,
                RecoveryPointArn=recovery_point_arn
            )

            #If resource type EC2, start the restore process
            if resource_type == 'EC2':
                #Before restore, change some restore metadata (AZ, Subnet & SG)
                metadata['RestoreMetadata']['CpuOptions'] = '{}'
                metadata['RestoreMetadata']['NetworkInterfaces'] = '[]'
                metadata['RestoreMetadata']['Placement'] = '{\"AvailabilityZone\":\"us-east-1b\",\"GroupName\":\"\",\"Tenancy\":\"default\"}'
                
                #Please mention the subnet id and security group id that you need to restore the instance
                metadata['RestoreMetadata']['SubnetId'] = 'subnet-0d402b67fa2b22396'
                metadata['RestoreMetadata']['SecurityGroupIds'] = '["sg-0a3403ld9b0341f31"]'
                
                #Start the restore job
                print('Starting the restore job...')
                restore_request = backup.start_restore_job(
                    RecoveryPointArn=recovery_point_arn,
                    IamRoleArn=iam_role_arn,
                    Metadata=metadata['RestoreMetadata']
                )
                return
            else:
                print('Unknown Resource Type')
                return
        
        elif job_type == 'Restore':
            
            restore_timestamp = int(datetime.timestamp(datetime.now()))
            
            #Get the Restore Job ID from SNS Message
            restore_job_id = event_object['Message'].split('.')[-1].split(':')[1].strip()
            topic_arn = event_object['TopicArn']
            
            #Getting the Original Instance Details
            original_instance_id = event_object['Message'].split('.')[-2].split(':')[6].split('/')[1].strip()
            original_instance_name = get_instanceName(original_instance_id)
            
            #Get the Rrestore Job more details
            restore_info = backup.describe_restore_job(
                RestoreJobId=restore_job_id
            )
            resource_type = restore_info['CreatedResourceArn'].split(':')[2]
            instance_id = restore_info['CreatedResourceArn'].split(':')[5].split('/')[1]
            
            #Store the Status on DynamoDB
            #Uncomment the below line if you need to send the job status to DynamoDB
            #put_itemDynamodb(restore_timestamp, original_instance_id, original_instance_name, restore_job_id, job_type, "Success")
            
            # Add Name Tag for Restored Instance
            response = ec2.create_tags(
                Resources=[
                    instance_id,
                ],
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': 'RestoreTestInstance',
                    },
                ],
            )

            if resource_type == 'ec2':
                
                ec2_resource_type = restore_info['CreatedResourceArn'].split(':')[5].split('/')[0]
                
                if ec2_resource_type == 'instance':
                    print('Test the Restoed instance before deletion')

                    #Get the Restored Instance Details
                    instance_details = ec2.describe_instances(
                        InstanceIds=[
                            instance_id
                        ]
                    )
                    
                    #Check the SSH Port is listening (Assume that server is properly bootup)
                    private_ip = instance_details['Reservations'][0]['Instances'][0]['PrivateIpAddress']
                    port = 22
                    
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5.0)
                        result = sock.connect_ex((private_ip,port))
   
                        if result == 0:
                            
                            test_timestamp = int(datetime.timestamp(datetime.now()))

                            print('SSH Connectivity OK to the Instance '+instance_id+'.')
                            print('Restore from the backup was SUCCEEDED & Validated. Deleting the newly created resource to save cost.')
                            
                            #Save the Status on DynamoDB
                            #Uncomment the below line if you need to send the job status to DynamoDB
                            #put_itemDynamodb(test_timestamp, original_instance_id, original_instance_name, restore_job_id, "RestoreTest", "Success")
                            
                            #Terminate the restored instances and attached volumes
                            volume_id_list = get_volume_id(instance_id)
                            delete_volumes(volume_id_list, instance_id)
                            terminate_instance(instance_id)
                            
                            subject = 'AWS Backup Restore Test SUCCEEDED on '+original_instance_name+'/'+original_instance_id
                            message = 'Restore from ' + restore_info['RecoveryPointArn'] + ' was successful and restore test SUCCEEDED with SSH OK returned by the instance '+instance_id+'. ' + 'The newly created resource ' + restore_info['CreatedResourceArn'] + ' has been cleaned up.'
                        else:
                            test_timestamp = int(datetime.timestamp(datetime.now()))
                            
                            print('SSH Connectivity FAILED to the Instance '+instance_id+'. Restore Test FAILED.')
                            print('Restore from the backup was FAILED. Deleting the newly created resource to save cost.')

                            #Terminate the restored instances and attached volumes
                            volume_id_list = get_volume_id(instance_id)
                            delete_volumes(volume_id_list, instance_id)
                            terminate_instance(instance_id)
                            
                            #Store the Status on DynamoDB
                            #Uncomment the below line if you need to send the job status to DynamoDB
                            #put_itemDynamodb(test_timestamp, original_instance_id, original_instance_name, restore_job_id, "RestoreTest", "FAILED")
                            
                            subject = 'AWS Backup Restore Test FAILED on '+original_instance_name+'/'+original_instance_id
                            message = 'Unable to connect to EC2 Instance ' + instance_id + ' via SSH. Restore test FAILED. New resource ' + restore_info['CreatedResourceArn'] + ' has been cleaned up. Please take immidiate action to sort it out.'
                        
                    except Exception as e:
                        print(str(e))
                        subject = 'AWS Backup Restore Test ERROR'
                        message = 'Error connecting to the Restored Instance: ' + str(e)
                
            else:
                print('Unknown Resource Type')
                return
                
            #Finally Send the Notification via SNS
            print('Sending the Restore Test Status Notification')
            sns = boto3.client('sns')
            notify = sns.publish(
                TopicArn=topic_arn,
                Message=message,
                Subject=subject
            )
            return
    except Exception as e:
        print(str(e))
        return