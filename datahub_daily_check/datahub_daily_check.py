'''
    This Lambda function is triggered when a file lands in s3 bucket coursera-degrees-data (DataHub)
    via a CloudWatch trigger
    Full Documentation:
        https://docs.google.com/document/d/1upeijCkhxl_cCkm9szVsBrX56DkSt85sLoWMxc5F1Hs/edit?ts=5c985907#heading=h.2gaogjueytny
    deployed to:
        https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions/datahub_daily_check
    For this function to run successfly:
        - A lambda layer has been created:
            https://console.aws.amazon.com/lambda/home?region=us-east-1#/layers/datahub_validator_lambda_layer/versions/1
        - With requirements file:
            https://github.com/webedx-spark/di-services/tree/master/lambda/datahub/datahub_daily_check/requirements.txt
        - To add a lambda layer:
            https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html
        - Specifically for this lambda:
            1. Install docker application
            2. Create a folder on your local machine add the following:
                1. requirements.txt
                2. folder call it "python"
                3. file call it  "bash.sh"
            3. go to the terminal switch to the folder
            4. execute the command "bash build.sh"
            5. zip the python file
            6. add the python.zip file to the lambda layer and select python 3.7 under compartible runtime.
            7. go to the datahub_daily_check and select layers and under select the layer you created.
    To do:
        1. Add backdating feature to remove static code for macquarie application file.
'''

import pandas as pd
import boto3
import logging


def main(event, context):
    """ This method is the Lambda event handler triggered by
        DatahubFileScheduleCheck event rule in cloud watch

        Parameters
        ----------
            event: dictionary:
                Contains event payload
            context:
                Contains context payload for the event
    """
    bucket_name = "coursera-degrees-data"
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(name=bucket_name)

    error = "wrong Data"
    metadata_file = "partner_schedule.csv"

    date_placeholder = event["time"][:10].replace("-", "") + ".csv"
    time_check = event["time"][11:16]

    try:

        partner_schedule = get_partner_shedule(metadata_file)

        partner_files, time_schedule = get_partner_files(
            partner_schedule, date_placeholder)

        check_all_bucket_folders(
            partner_files,
            time_check,
            time_schedule,
            partner_schedule,
            s3_bucket)

        error = "Success"
    except BaseException as e:
        logging.exception(e)
        return (" ".join(["Meta Data File",
                          "'" + metadata_file + " is missing",
                          str(e)]))

    return(error)


def get_partner_shedule(metadata_file):
    """ This method gets the partners' schedules.

        Parameters
        ----------
            metadata_file: string
                name of the csv metadata file
        return
        ------
            partner_schedule: dataframe
                contains partner schedule info
    """
    partner_schedule = pd.read_csv(metadata_file, encoding="utf8")
    partner_schedule = partner_schedule.dropna(
        axis=0, how='all', thresh=None, subset=None, inplace=False)

    # format dataframe data to string removing any white spaces
    partner_schedule['run_hour'] = partner_schedule['run_hour'].astype(
        str).replace(" ", "")
    partner_schedule['programs'] = partner_schedule['programs'].astype(
        str).replace(" ", "")
    partner_schedule['internal_emails'] = partner_schedule['internal_emails'].astype(
        str).replace(" ", "")
    partner_schedule['partner_emails'] = partner_schedule['partner_emails'].astype(
        str).replace(" ", "")
    partner_schedule['ignore_files'] = partner_schedule['ignore_files'].astype(
        str).replace(" ", "")
    partner_schedule['swap_files'] = partner_schedule['swap_files'].astype(
        str).replace(" ", "")

    return partner_schedule


def get_partner_files(partner_schedule, date_placeholder):
    """
        Parameters
        ----------
            partner_schedule: dataframe
                contains partner schedule info
            date_placeholder : str
                The date place holder: yyyymmdd.csv

        return
        ------
            partner_files: dataframe
                contains formatted partner files info
            time_schedule: dataframe
                contains times to run the partner programs
    """
    time_schedule = pd.DataFrame(columns=['partner', 'run_hour'])
    partner_files = pd.DataFrame(columns=['partner', 'files'])

    for _, row in partner_schedule.iterrows():
        row['programs'] = [str(row['partner']) + "/" + str(x)
                           for x in row['programs'].split(",") if x != '']

        swap_files = [i.split(",") for i in row['swap_files'].split(
            ":") if row['swap_files'] != 'nan']

        for program in row['programs']:
            ignore_files = []
            for ignore_program in row['ignore_files'].split('|'):
                if program == str(row['partner']) + "/" + ignore_program.split(':')[0]:
                    for j in ignore_program.split(':')[1].split(','):
                        ignore_files.append(j)
                    break
            files_to_check = get_files_to_check(
                program, date_placeholder, ignore_files, swap_files)

            for file in files_to_check:

                # backdate macquarie application file to previous day
                if "macquarie/mq-global-mba/applications/applications" in file:
                    file = file.replace(
                        file[-12:-4], str(int(file[-12:-4]) - 1))

                partner_files = partner_files.append(
                    {'partner': row['partner'], 'files': file}, ignore_index=True)

        row['run_hour'] = [
            x for x in row['run_hour'].split(",") if x != '']
        for x in row['run_hour']:
            time_schedule = time_schedule.append(
                {'partner': row['partner'], 'run_hour': x}, ignore_index=True)

    return (partner_files, time_schedule)


def get_files_to_check(program, date_placeholder, ignore_files, swap_files):
    """ This returns a list of files to check

    Parameters
    ----------
        program : str
            The name of the degree program
        date_placeholder : str
            The date place holder: yyyymmdd.csv
        ignore_files: list
            list of files to ignore
        swap_files: list
            list of files to swap
    return
    ------
        files: list of files
    """
    file_names = [
        'applications',
        'terms',
        'students',
        'degree_terms_courses',
        'degree_term_memberships',
        'degree_program_memberships',
        'degree_courses',
        'degree_course_memberships',
    ]

    # Swap for any files that are not named according to standard eg. Penn
    if len(swap_files) > 0:
        for swap in swap_files:
            for i, file in enumerate(file_names):
                if swap[0] == file:
                    file_names[i] = swap[1]
                    break

    # remove any files to ignore
    file_names = [x for x in file_names if x not in ignore_files]

    files = [
        '/applications/' + i + '_'
        if i == 'applications' else '/enrollments/' + i + '_'
        for i in file_names
    ]
    files = [program + i + date_placeholder for i in files]

    return(files)


def check_all_bucket_folders(partner_files, time_check, time_schedule, partner_schedule, s3_bucket):
    """This returns a list of files to check

        Parameters
        ----------
            partner_files : str
                The name of the degree program
            time_check : str
                The date place holder: yyyymmdd.csv
            time_schedule: list
                list of files to ignore
            partner_schedule: list
                list of files to swap
            s3_bucket:
                S3 bucket where the folders and file are located
        return
        ----------
            files: list of files
    """
    bucket_prefixes = partner_files['partner'].drop_duplicates(
        keep='first', inplace=False)
    bucket_prefixes = bucket_prefixes.dropna()
    files = []

    # loop through partner prefix folders
    for prefix in bucket_prefixes.values.tolist():
        df2 = partner_files
        log = ['Files not received for ' + prefix]
        log.append('Time Check: ' + time_check + ' (UTC): Reminder: ')

        time_in_file_Schedule = set(time_schedule['run_hour'].where(
            time_schedule['partner'] == prefix).dropna(axis='rows', how='all'))

        if time_check in time_in_file_Schedule:

            lstfiles = set(df2['files'].where(
                df2['partner'] == prefix).dropna(axis='rows', how='all'))

            internal_emails = partner_schedule['internal_emails'].where(
                partner_schedule['partner'] == prefix).dropna(axis='rows', how='all').values.tolist()[0]
            external_emails = partner_schedule['partner_emails'].where(
                partner_schedule['partner'] == prefix).dropna(axis='rows', how='all').values.tolist()[0]

            files = set(
                y for y in [v.key for v in s3_bucket.objects.filter(Prefix=prefix)])

            inter = set(lstfiles & files)
            not_sent = [x for x in lstfiles if x not in inter]

            if len(not_sent) > 0:
                msg = "".join(
                    ["\n\t\t" + x for x in lstfiles if x not in inter])
                log.append(msg)

                toEmails = [[], []]
                if len(str(external_emails).strip()) > 3:
                    toEmails[0] = external_emails.split(';')
                if len(str(internal_emails).strip()) > 3:
                    toEmails[1] = internal_emails.split(';')
                send_email(toEmails, log)


def send_email(toEmails, log, email_type=""):
    """ This method sends an email

    Parameters
    ----------
        sender_email : str
            The name of the sender email
        receiver_email : str or List
            The name of the receipient email(s)
        log: list
            This is contains the log information to send
    """
    fromEmail = "datahub@coursera.org"
    replyTo = "datahub@coursera.org"
    subject = "Coursera Data Exchange Automated Alert: " + log[0]
    log[1] += log[0].replace("received", "sent")
    message = get_email_message(log)
    client = boto3.client('ses')

    response = client.send_email(
        Source=fromEmail,
        Destination={
            'ToAddresses': toEmails[0],
            'BccAddresses': toEmails[1]
        },
        Message={
            'Subject': {
                'Data': subject,
                'Charset': 'utf8'
            },
            'Body': {
                'Text': {
                    'Data': message,
                    'Charset': 'utf8'
                }
            }
        },
        ReplyToAddresses=[
            replyTo
        ]
    )

    print(message, response)
    return {'code': 0, 'message': 'success'}


def get_email_message(log):
    """ This function sends constructs the email message
    return:
        message: string
    """
    message = "\n\nThank you for your partnership in data exchange with Coursera."
    message += "\nPlease review the below issue(s) "
    message += "to ensure our platform can achieve our target "
    message += "reliability goals for this program:\n\n"
    message += ''.join(log[1:])
    message += "\n\nThis email is not monitored. For any questions relating to Datahub,"
    message += " please email your Coursera Partner Product Specialist."
    message += "\nIf you're not sure who that is, please reach out to "
    message += "partner-support@coursera.org to find your Partner Product Specialist."

    return message
