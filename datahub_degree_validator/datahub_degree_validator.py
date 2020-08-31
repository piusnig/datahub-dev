"""
    This script is the DataHub Data Validator:
    So Once a new file lands in s3 DataHub bucket, we’d like to validate automatically:
        1. Filenames are per spec
        2. Structure (ie fields)
        3. Content (ie rows and datatypes)

    Any invalidations are:
        1. Flagged and logged
        2. Alerted back to the Partner
        3. Prevent the file from processing further
"""
import csv
import logging
import os
import re
import tempfile
from collections import OrderedDict
from datetime import datetime
from io import StringIO, BytesIO

import boto3
import botocore
import pandas as pd


def lambda_handler(event, context=None):
    try:

        settings = Settings()

        partner_bucket = event["detail"]["requestParameters"]["bucketName"]

        validate_file = ValidateFile(partner_bucket)

        # Check if file should be processed
        file_path_list = validate_file.validate_lambda_event(event, settings)
        if file_path_list:

            # file object
            file = File(file_path_list)

            # set Settings
            settings.set_file_settings()

            # initiate error logging
            error_logs = ErrorLogging(settings.ps_data)
            log = OrderedDict()

            # 1. check file name
            status = validate_file.validate_file_name(file_path_list, settings.mt_data)

            if status == "Success":

                # reset settings for secific to the file
                settings.set_file_settings(file.file_path_no_ext)

                # 2. check if file is empty
                status = validate_file.validate_file_empty(file)

                if status == "Success":

                    # 3. check file column structure
                    status = validate_file.validate_file_column_structure(
                        file, settings.mt_data
                    )

                    if status == "Success":

                        # 4. check file column data types
                        status = validate_file.validate_field_datatypes(file, settings)

                        if status == "Success":

                            # 5. check pk violation
                            status = validate_file.validate_file_pk_violation(
                                file, settings
                            )
                            if status != "Success":
                                log = error_logs.log_info_file_pk_violation(
                                    file, status
                                )

                        else:

                            log = error_logs.log_info_wrong_field_datatypes(
                                file, status
                            )

                    else:
                        # logs for wrong_file_structure
                        log = error_logs.log_info_wrong_file_structure(file, status)
                else:
                    log = error_logs.log_info_file_empty(file, status)
            else:
                # logs for wrong_file_name
                log = error_logs.log_info_wrong_file_name(file, status)

            if log:
                pass

                # send_email
                # SendEmail().send_email(log)

                # add logs to logs_bucket
                error_logs.add_logs_to_bucker(settings.logs_bucket, log)
            print(file.file_path, ": status: ", status)
            return status

    except KeyError:
        raise KeyError(f"Wrong lambda event Key supplied {KeyError}")


class ValidateFile:
    """This class validates a partner file

    Attributes:
        partner_bucket (str): bucket where the partner folders and files are located
        file (File Object): That will store file object
    """

    def __init__(self, partner_bucket="coursera-degrees-data"):
        self.file = None
        self.partner_bucket = partner_bucket

    def validate_lambda_event(self, event, settings):
        """This method validates a lambda file event

        Attributes:
            even t(dict): for lambda event
            settings (object): settings used during execution

        return:
            if correct event: file_path_list (list)             
            else: None:
        """

        file_path_list = event["detail"]["requestParameters"]["key"]
        file_path_list = list(map(str.lower, file_path_list.split("/")))
        try:

            # file path list => [parner, program_slug, enrollments_or_applications, filename.csv]
            # file_path =>  parner/program_slug/enrollments_or_applications/filename.csv

            if (
                len(file_path_list) == 4
                and file_path_list[2] in settings.partner_folders
            ):
                excepted_files = [
                    "finance_transactions",
                    "finance_metadata",
                ]
                mt_data = settings.get_metadata()

                valid_folder_paths = set(mt_data["folder_path"])

                # check if the file folder path is in metadata folder_path
                if (
                    "/".join(file_path_list[:3]) in valid_folder_paths
                    and "_".join(file_path_list[3].split("_")[:-1])
                    not in excepted_files
                ):
                    return file_path_list
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_lambda_event: "
                + str(e)
            )
        return

    def validate_file_name(self, file_path_list, mt_data):
        """This method validates the file name

        Attributes:
            file_path_list (list): [parner, program_slug, enrollments_or_applications, filename.csv]
            mt_data (dataframe): from settings metadata file

        return:
            if valid file name: message (str): Success               
            else: message (dict)
        """
        try:
            file_name = file_path_list[3].split("_")
            file_prefix = file_path_list[:3] + ["_".join(file_name[:-1])]
            file_prefix = "/".join(file_prefix)
            mt_file_prefix = list(
                mt_data[mt_data["file_prefix"] == file_prefix][
                    "file_prefix"
                ].drop_duplicates(keep="first")
            )
            message = "Success"
            file_names = [
                "applications",
                "degree_course_memberships",
                "degree_courses",
                "degree_terms_courses",
                "degree_program_memberships",
                "degree_term_memberships",
                "terms",
                "students",
            ]
            if (
                len(file_name[-1].split(".")) != 2
                or not mt_file_prefix
                or not "csv" == file_name[-1].split(".")[-1]
                or not re.match(r"\d{4}\d{2}\d{2}$", file_name[-1].split(".")[0])
                or "_".join(file_path_list[3].split("_")[:-1]) not in file_names
            ):

                message = {"error_code": 1, "file_path": file_path_list}
                return message
            return message
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_file_name: " + str(e)
            )

    def validate_file_column_structure(self, file, mt_data):
        """This method validates the file column structure

        Attributes:
            file (File Object): stores file object
            mt_data (dataframe): from settings metadata file

        return:
            if valid file name: message (str): Success               
            else: message (dict)
        """
        try:
            file_structure_cols = list(
                mt_data[mt_data["file_prefix"] == file.file_path_no_ext]["field"]
            )

            file_data = BucketFileData().read_csv(self.partner_bucket, file.file_path)

            file.file_no_of_rows = file_data.shape[0]
            message = "Success"

            if file_structure_cols != list(file_data.columns):
                message = {
                    "error_code": 2,
                    "supplied_fields": list(file_data.columns),
                    "expected_fields": file_structure_cols,
                }
            return message
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_file_column_structure: "
                + str(e)
            )

    def validate_field_datatypes(self, file, settings):
        """This method validates the file column datatypes

        Attributes:
            file (File Object): stores file object
            fn_data (dataframe): from settings file_names file

        return:
            if valid file name: message (str): Success               
            else: message (dict)
        """
        try:
            fn_data = settings.fn_data
            mt_data = settings.mt_data

            file_data = BucketFileData().read_csv(self.partner_bucket, file.file_path)
            message = []

            for _, field in fn_data.iterrows():
                if field["field"].lower() in file_data.columns:
                    mt_regex = mt_data[mt_data["field"] == field["field"].lower()]
                    field_regex = settings.get_field_regex(
                        mt_regex["unique_data_type"].values[0],
                        mt_regex["unique_length"].values[0],
                        mt_regex["unique_mandatory_values"].values[0],
                    )
                    if not field_regex:
                        field_regex = field["field_regex"]
                    lst = list(
                        filter(
                            re.compile(field_regex).match,
                            [str(x) for x in list(file_data[field["field"]])],
                        )
                    )
                    exceptns = {
                        field["field"]: [i if i != "nan" else "null" for i in lst]
                    }
                    if len(exceptns[field["field"]]) > 0:
                        message.append(exceptns)
            if message:
                message = {"error_code": 4, "exceptions": message}
                return message

            return "Success"
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_field_datatypes: "
                + file.file_path
                + str(e)
            )

    def validate_file_empty(self, file):

        """This method validates is the file is empty

        Attributes:
            file (File Object): stores file object

        return:
            if file is not empty: message (str): Success               
            else: message (dict)
        """
        try:
            message = "Success"
            file_data = BucketFileData().read_csv(self.partner_bucket, file.file_path)
            if file_data is None or file_data.empty:
                message = {"error_code": 3}
            return message
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_file_empty: " + str(e)
            )

    def validate_file_pk_violation(self, file, settings):
        try:
            fn_data = settings.fn_data
            mt_data = settings.mt_data

            file_data = BucketFileData().read_csv(self.partner_bucket, file.file_path)
            message = "Success"

            pk_cols = mt_data[mt_data["unique_pk"] == 1]["field"].values.tolist()
            if not pk_cols:
                pk_cols = fn_data[fn_data["pk"] == 1]["field"].values.tolist()

            pks_rows = file_data.pivot_table(
                index=pk_cols, aggfunc="size"
            ).reset_index()
            pks_rows.columns = [*pks_rows.columns[:-1], "No"]
            pks_rows = pks_rows[pks_rows["No"] > 1]

            if not pks_rows.empty:
                message = {"error_code": 5, "pks_rows": pks_rows}
            return message
        except BaseException as e:
            print(
                "exception: class ValidateFile: Method: validate_file_pk_violation: "
                + file.file_path
                + str(e)
            )


class ErrorLogging:
    """This class logs information

    Attributes:
        partner_schedule (str): bucket where the partner folders and files are located
        error_types (dict): different error types
        date_time = date timestamp
        date = date for adding to log file
        cols (tuple): columns for the object in order

    """

    def __init__(self, partner_schedule):
        self.partner_schedule = partner_schedule
        self.error_types = {
            1: {"priority": "CRITICAL", "description": "wrong file name",},
            2: {"priority": "CRITICAL", "description": "wrong file structure",},
            3: {"priority": "CRITICAL", "description": "empty file",},
            4: {"priority": "CRITICAL", "description": "wrong field data types",},
            5: {"priority": "URGENT", "description": "PK Violation",},
        }
        self.date_time = datetime.utcnow()
        self.date = str(self.date_time.strftime("%Y%m%d"))
        self.cols = self.get_error_cols()

    def log_info_wrong_file_name(self, file, error_log):
        """This method creates the log structure for wrong file name

        Attributes:
            file (File Object): stores file object
            error_log (dict): error data for formating

        return:
            log (dict): formatted error log
        """
        try:
            log = {}
            log["error_code"] = error_log["error_code"]
            log["error_type"] = (
                self.error_types[error_log["error_code"]]["description"]
                if error_log["error_code"] in self.error_types
                else "N/A"
            )
            log["priority"] = self.error_types[error_log["error_code"]]["priority"]
            log = self.add_common_fields_to_log(log, file)
            log["description"] = self.add_log_description(log)
            log = self.reorder_log(log)

            return log
        except BaseException as e:
            file_empty(
                "exception: class ErrorLogging: Method: log_info_wrong_file_name: "
                + str(e)
            )

    def log_info_wrong_field_datatypes(self, file, error_log):
        """This method creates the log structure for wrong field datatypes

        Attributes:
            file (File Object): stores file object
            error_log (dict): error data for formating

        return:
            log (dict): formatted error log
        """
        try:
            log = {}
            log["error_code"] = error_log["error_code"]
            log["error_type"] = (
                self.error_types[error_log["error_code"]]["description"]
                if error_log["error_code"] in self.error_types
                else "N/A"
            )
            log["description"] = error_log["exceptions"]
            log["priority"] = self.error_types[error_log["error_code"]]["priority"]
            log = self.add_common_fields_to_log(log, file)
            log["description"] = self.add_log_description(log)
            log = self.reorder_log(log)
            return log
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: log_info_wrong_field_datatypes: "
                + str(e)
            )

    def log_info_file_empty(self, file, error_log):
        """This method creates the log structure for wrong file structure

        Attributes:
            file (File Object): stores file object
            error_log (dict): error data for formating

        return:
            log (dict): formatted error log
        """
        try:
            log = {}
            log["error_code"] = error_log["error_code"]
            log["error_type"] = (
                self.error_types[error_log["error_code"]]["description"]
                if error_log["error_code"] in self.error_types
                else "N/A"
            )
            log["description"] = self.add_log_description(log)
            log["priority"] = self.error_types[error_log["error_code"]]["priority"]
            log = self.add_common_fields_to_log(log, file)
            log = self.reorder_log(log)
            return log
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: log_info_file_empty: " + str(e)
            )

    def log_info_wrong_file_structure(self, file, error_log):
        """This method creates the log structure for wrong file structure

        Attributes:
            file (File Object): stores file object
            error_log (dict): error data for formating

        return:
            log (dict): formatted error log
        """
        try:
            log = {}
            log["error_code"] = error_log["error_code"]
            log["error_type"] = (
                self.error_types[error_log["error_code"]]["description"]
                if error_log["error_code"] in self.error_types
                else "N/A"
            )
            log["supplied_fields"] = error_log["supplied_fields"]
            log["expected_fields"] = error_log["expected_fields"]
            log["no_supplied_fields"] = str(len(error_log["supplied_fields"]))
            log["no_expected_fields"] = str(len(error_log["expected_fields"]))
            log[
                "description"
            ] = f"\n\t{ file.file_name}: Number of Rows:  {file.file_no_of_rows}"
            log["description"] += self.add_log_description(log)
            log["priority"] = self.error_types[error_log["error_code"]]["priority"]
            log = self.add_common_fields_to_log(log, file)
            log["supplied_fields"] = ",".join(log["supplied_fields"])
            log["expected_fields"] = ",".join(log["expected_fields"])

            return self.reorder_log(log)
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: log_info_wrong_file_structure: "
                + str(e)
            )

    def log_info_file_pk_violation(self, file, error_log):
        """This method creates the log structure for wrong field datatypes

        Attributes:
            file (File Object): stores file object
            error_log (dict): error data for formating

        return:
            log (dict): formatted error log
        """
        try:
            log = {}
            log["error_code"] = error_log["error_code"]
            log["error_type"] = (
                self.error_types[error_log["error_code"]]["description"]
                if error_log["error_code"] in self.error_types
                else "N/A"
            )
            log["description"] = error_log["pks_rows"]
            log["priority"] = self.error_types[error_log["error_code"]]["priority"]
            log = self.add_common_fields_to_log(log, file)
            log["description"] = self.add_log_description(log)
            log = self.reorder_log(log)
            return log
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: log_info_wrong_field_datatypes: "
                + str(e)
            )

    def add_common_fields_to_log(self, log, file):
        try:
            log["partner"] = file.partner_slug
            log["program"] = file.program_slug
            log["file_name"] = file.file_name
            log["file_path"] = file.file_path
            log["file_no_of_rows"] = file.file_no_of_rows
            log["log_file_name"] = "_".join(
                [
                    "datahub_logs",
                    file.partner_slug,
                    file.program_slug,
                    "log",
                    self.date + ".csv",
                ]
            )

            log["log_file_path"] = "/".join(
                [
                    "datahub/datahub_validator/logs",
                    log["partner"],
                    log["program"],
                    log["log_file_name"],
                ]
            )
            log["date_time"] = self.date_time
            self.partner_schedule = self.partner_schedule[
                self.partner_schedule["partner"] == file.partner_slug
            ]

            log["partner_emails"] = (
                str(self.partner_schedule["partner_emails"].values.tolist()[0])
                .replace("nan", "")
                .strip()
            )
            log["internal_emails"] = (
                str(self.partner_schedule["internal_emails"].values.tolist()[0])
                .replace("nan", "")
                .strip()
            )

            return log
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: add_common_fields_to_log: "
                + str(e)
            )

    def add_log_description(self, log):
        try:
            desc = ""
            if log["error_code"] == 1:
                desc = " ".join(
                    [
                        "\n\t" + log["file_name"],
                        ": File wrongly Named and cannot be processed.",
                    ]
                )
            elif log["error_code"] == 2:  # file Structure
                if log["no_supplied_fields"] != log["no_expected_fields"]:
                    desc += " ".join(
                        [
                            "\n\t\tsupplied: ",
                            log["no_supplied_fields"],
                            "fields instead of: ",
                            log["no_expected_fields"],
                            "fields",
                        ]
                    )
                    if set(log["expected_fields"]) - set(log["supplied_fields"]):
                        desc += ".\n\t\tMissing fields: "
                        desc += ", ".join(
                            set(log["expected_fields"]) - set(log["supplied_fields"])
                        )
                    if set(log["supplied_fields"]) - set(log["expected_fields"]):
                        desc += ".\n\t\tWrong fields supplied: "
                        desc += ", ".join(
                            set(log["supplied_fields"]) - set(log["expected_fields"])
                        )
                else:
                    for i, _ in enumerate(log["supplied_fields"]):
                        if log["supplied_fields"][i] != log["expected_fields"][i]:
                            desc += " ".join(
                                [
                                    "\n\t\tsupplied: ",
                                    "'" + log["supplied_fields"][i] + "'",
                                    "instead of: ",
                                    "'" + "".join(log["expected_fields"][i]) + "'",
                                ]
                            )

                    desc += ".\n\tFile is poorly formated and cannot be processed."
            elif log["error_code"] == 3:  # file Empty
                desc = ": empty file sent"
            elif log["error_code"] == 4:  # field datatypes
                for exceptns in log["description"]:
                    for field in exceptns:
                        desc += ": ".join(
                            [
                                "\n\tMandatory Field",
                                field,
                                "Number of Exceptions",
                                str(len(exceptns[field])),
                                "wrong values include",
                                ", ".join(exceptns[field][:3]) + "....",
                            ]
                        )
            elif log["error_code"] == 5:  # field pk violation
                cols = list(log["description"].columns)
                desc = (
                    "\n\tDuplicates in Primary Key columns: "
                    + ", ".join(cols[:-1])
                    + ": Number: "
                    + str(log["description"][cols[-1]].sum())
                )

                row_list = []
                for index, rows in log["description"].iterrows():
                    if index <= 2:
                        desc += "\n\t\tField values: " + (
                            ", ".join(map(str, list(rows[cols[:-1]])))
                            + ": Number: "
                            + str(rows[cols[-1]])
                        )
                    else:
                        break
            return desc
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: add_log_description: " + str(e)
            )

    def add_logs_to_bucker(self, logs_bucket, log):
        try:
            "datahub_error_logs_partner_program_file_date.csv"

            bfd = BucketFileData()
            # file_path = log["log_file_name"]
            file_path = log["log_file_path"]

            df = pd.DataFrame(log, index=[0])
            file_df = bfd.read_csv(logs_bucket, file_path)

            if file_df is not None:
                file_df = pd.concat([file_df[self.cols], df[self.cols]])[self.cols]
            else:
                file_df = df[self.cols]
            bfd.upload_csv(logs_bucket, file_df, file_path)
            return file_df
        except BaseException as e:
            print(
                "exception: class ErrorLogging: Method: add_logs_to_bucker: " + str(e)
            )

    def reorder_log(self, error_log):
        log = OrderedDict()
        for col in self.cols:
            if col not in error_log.keys():
                log[col] = ""
            else:
                log[col] = error_log[col]
        return log

    def get_error_cols(self):
        cols = [
            "error_code",
            "error_type",
            "supplied_fields",
            "expected_fields",
            "no_supplied_fields",
            "no_expected_fields",
            "description",
            "partner",
            "program",
            "file_name",
            "file_path",
            "file_no_of_rows",
            "log_file_name",
            "log_file_path",
            "priority",
            "partner_emails",
            "internal_emails",
            "date_time",
        ]
        return cols


class Settings:
    """ This sets settings for the parameters needed the metadata sheet """

    def __init__(self,):
        self.settings_bucket = "coursera-data-engineering"
        self.logs_bucket = "coursera-data-engineering"
        folder = "datahub/datahub_validator/settings/"
        self.metadata_file = folder + "metadata.csv"
        self.fieldnames_file = folder + "fieldnames.csv"
        self.partner_schedule_file = folder + "partner_schedule.csv"
        self.partner_folders = ("enrollments", "applications")
        self.mt_data = None
        self.ps_data = None
        self.fn_data = None
        self.cls_read_file = BucketFileData()

    def get_metadata(self, mt_data=None):
        try:
            mt_data = self.cls_read_file.read_csv(
                self.settings_bucket, self.metadata_file
            ).sort_values(by=["row_id"])
            cols = ["partner", "program", "folder", "file"]
            mt_data["folder_path"] = mt_data[cols[:3]].apply(
                lambda row: "/".join(row), axis=1
            )
            mt_data["file_prefix"] = mt_data[cols].apply(lambda i: "/".join(i), axis=1)
            return mt_data
        except BaseException as e:
            print("exception: class Settings: Method: get_metadata: " + str(e))

    def get_partner_schedule(self):
        try:
            ps_data = self.cls_read_file.read_csv(
                self.settings_bucket, self.partner_schedule_file
            )
            return ps_data
        except BaseException as e:
            print("exception: class Settings: Method: get_partner_schedule: " + str(e))

    def get_fieldnames(self, fn_data=None):
        try:
            fn_data = self.cls_read_file.read_csv(
                self.settings_bucket, self.fieldnames_file
            )
            fn_data["field_regex"] = fn_data.apply(
                lambda row: self.get_field_regex(
                    row["data_type"], row["length"], row["mandatory_values"]
                ),
                axis=1,
            )
            return fn_data
        except BaseException as e:
            print("exception: class Settings: Method: get_fieldnames: " + str(e))

    def set_file_settings(self, file_path_no_ext=None):
        mt_data = self.get_metadata()
        ps_data = self.get_partner_schedule()
        fn_data = self.get_fieldnames()
        if file_path_no_ext:
            mt_data = mt_data[mt_data["file_prefix"] == file_path_no_ext]
            ps_data = ps_data[ps_data["partner"] == file_path_no_ext.split("/")[0]]
            fn_data = fn_data[fn_data["file"] == file_path_no_ext.split("/")[3]]
        self.mt_data = mt_data
        self.ps_data = ps_data
        self.fn_data = fn_data

    def get_valid_file(self):
        files = [
            "applications",
            "degree_course_memberships",
            "degree_courses",
            "degree_terms_courses",
            "degree_program_memberships",
            "degree_term_memberships",
            "terms",
            "students",
        ]
        return files

    def get_field_regex(self, data_type, length, mandatory_values):
        try:
            regex_dict = {
                "EMAIL": r"^(?!([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]))",
                "EMAIL2": r"^(?!([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]|))",
                "DATE": r"^(?!((\d{4}-\d{2}-\d{2})$))",
                "VARCHAR": r"(?!([^\W_]$|))",
                "VARCHAR1": r"^(?!([\W_]))$|nan",  # string do not allow nulls
                # with length
                "VARCHAR2": r"^(?!([a-zA-Z]{" + str(length).replace(".0", "") + "}))",
                "VARCHAR3": r"(?!([^\W_]$|))",  # string allow nulls
                # Variable with options
                "INT": r"^(?!(\d+$))",
                "TIMESTAMP": r"^(?!((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|nan|(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+)$))",
            }
            if str(mandatory_values) != "nan":
                regex_dict.update(
                    {
                        "VARCHAROPTNS": r"^(?!("
                        + str(mandatory_values.split(","))
                        .strip()
                        .replace("\\", "")
                        .replace(",", "$|^")
                        .replace("'", "")
                        .replace("|^ ", "|^")
                        .replace("[", "")
                        .replace("]", "")
                        + ")$)"
                    }
                )
            for key, regex in regex_dict.items():
                if key == data_type:
                    return regex
        except BaseException as e:
            print("exception: class Settings: Method: get_field_regex: " + str(e))


class BucketFileData:
    def __init__(self, s3=None):
        self.s3 = boto3.client("s3")

    def read_csv(self, bucket, file_path):
        """Function reads csv file into and returns a pandas data frame"""
        try:

            res = self.s3.get_object(Bucket=bucket, Key=file_path)["Body"]

            data_frame = pd.read_csv(
                BytesIO(res.read()),
                encoding="ISO-8859-1",
                keep_default_na=False,
                na_values=["NULL", ""],
            )
            data_frame.columns = map(str.lower, data_frame.columns)

            return data_frame
        except BaseException as e:
            print(
                "exception: class BucketFileData: Method: read_csv: bucket: "
                + file_path
                + ": "
                + str(e)
            )

    def upload_csv(self, bucket, file_df, s3_file_path):
        try:

            buffer = StringIO()
            file_df.to_csv(buffer, header=True, index=False, quoting=csv.QUOTE_ALL)
            buffer.seek(0)
            response = self.s3.put_object(
                Body=buffer.getvalue(), Bucket=bucket, Key=s3_file_path
            )

        except Exception as e:
            print(
                "exception: class BucketFileData: Method: upload_csv: bucket: " + str(e)
            )


class File:
    def __init__(self, file_path_list=None):
        """
        This class models the file object.
        """
        self.partner_slug = file_path_list[0]
        self.program_slug = file_path_list[1]
        self.folder_name = file_path_list[2]
        self.file_name = file_path_list[3]
        self.file_path = "/".join(file_path_list)
        self.file_date_stamp = file_path_list[-1].split("_")[-1]

        self.file_no_date_stamp_ext = file_path_list[3].replace(
            "_" + self.file_date_stamp, ""
        )
        self.file_metadata = pd.DataFrame()
        self.file_metadata_regex = pd.DataFrame()
        self.file_regex = self.set_file_regex(self.file_no_date_stamp_ext)
        self.file_path_no_ext = "/".join(
            file_path_list[:3] + [self.file_no_date_stamp_ext]
        )
        self.file_no_of_rows = None

    def set_file_regex(self, file_no_date_stamp_ext):
        regex = [
            "applications",
            "degree_course_memberships",
            "degree_courses",
            "degree_terms_courses",
            "degree_program_memberships",
            "degree_term_memberships",
            "terms",
            "students",
        ]
        for x in regex:
            if x == file_no_date_stamp_ext:
                return r"^" + x + r"_\d{4}\d{2}\d{2}.csv$"


class SendEmail:
    """
    This class constructs and sends email containing log information
    Methods:
    -------
    send_email
        This method sends an email
    """

    def send_email(self, log):
        """
        This method sends an email

        Parameters
        ----------
        sender_email: str
            The name of the sender email
        receiver_email: str or List
            The name of the receipient email(s)
        log: list
            This is contains the log information to send
        """
        fromEmail = "datahub@coursera.org"
        replyTo = "datahub@coursera.org"
        email = [[], []]
        subject = (
            log["priority"]
            + ": LAMBDA TEST: Coursera Data Exchange Automated Alert: "
            + "Please review file: "
            + log["file_name"]
            + " for Degree Program: "
            + log["program"]
        )
        if len(log["internal_emails"]) > 3:
            email[0] = log["internal_emails"].split(";")
        if len(log["partner_emails"]) > 3:
            email[1] = log["partner_emails"].split(";")

        message = self.get_email_message(log)
        client = boto3.client("ses")
        response = client.send_email(
            Source=fromEmail,
            Destination={"ToAddresses": email[1], "BccAddresses": email[0],},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": message, "Charset": "UTF-8"}},
            },
            # ReplyToAddresses=[replyTo],
        )
        print(message)
        return {"code": 0, "message": "success"}

    def get_email_message(self, log):
        """ This function constructs the email message
        return:
            message: string
        """
        message = "\n\nThank you for your partnership in data exchange with Coursera."
        message += "\nPlease review the below issue(s) "
        message += "to ensure our platform can achieve our target "
        message += "reliability goals for this program:" + "\n\n"
        message += ": ".join(
            [
                "Logs for Partner",
                log["partner"],
                "Degree Program",
                log["program"],
                log["file_name"],
            ]
        )
        if log["file_no_of_rows"]:
            message += ": Number of Rows: " + str(log["file_no_of_rows"])
        message += log["description"]
        message += (
            "\n\nThis email is not monitored. For any questions relating to Datahub,"
        )
        message += " please email your Coursera Partner Product Specialist."
        message += "\nIf you're not sure who that is, please reach out to "
        message += (
            "partner-support@coursera.org to find your Partner Product Specialist."
        )

        return message
