import boto3
import pytest
from moto import mock_s3, mock_ses
import datetime
from collections import OrderedDict

PartnerBucket = "coursera-degrees-data"


def get_files(file_types):
    set_files = {
        "correct_files": [
            "test/degree/enrollments/terms_20200128.csv",
            "test/degree/enrollments/degree_course_memberships_20200724.csv",
            "test/degree/enrollments/degree_term_memberships_20200805.csv",
        ],
        "wrong_files": ["test/degree/enrollment/terms_2020019.csv"],  # does not Exist
        "wrong_file_names": [
            "test/degree/enrollments/term_20200128.csv",  # wrong name term instead terms
            "test/degree/enrollments/terms_2020012.csv",  # wrong date
            "test/degree/enrollments/terms_2020012.csv",  # wrong date
            "test/degree/enrollments/terms_2020012.csv1",  # file extension
            "test/degree/enrollments/terms_2020014.csv1",  # file extension
            "test/degree/enrollments/terms_2020014.pdf",  # file extension
        ],
        "wrong_file_structure": [
            "test/degree/enrollments/terms_20200129.csv",  # fewer Columns
            "test/degree/enrollments/terms_20200127.csv",  # same No of Columns but wrong column name(s)
        ],
        "wrong_field_datatypes": ["test/degree/enrollments/terms_20200126.csv"],  #
        "file_not_exists": [
            "test/degree/enrollments/terms_20200130.csv",  # fewer Columns
        ],
        "file_empty": [
            "test/degree/enrollments/terms_20200124.csv",  # No Columns
            "test/degree/enrollments/terms_20200125.csv",  # with Columns
        ],
        "file_pk_violation": [
            "test/degree/enrollments/terms_20200123.csv",
            "test/degree/enrollments/degree_program_memberships_20200828.csv",
        ],
        "swap_files": ["test/degree/enrollments/degree_term_courses_20200830.csv"],
    }
    files = [
        {"detail": {"requestParameters": {"bucketName": PartnerBucket, "key": i}}}
        for i in set_files[file_types]
    ]
    return files


@pytest.fixture(params=get_files("correct_files"))
def correct_files_event(request):
    return request.param


@pytest.fixture(params=get_files("wrong_files"))
def wrong_files_event(request):
    return request.param


@pytest.fixture(params=get_files("wrong_file_names"))
def wrong_file_names_event(request):
    return request.param


@pytest.fixture(params=get_files("wrong_file_structure"))
def wrong_file_structure_event(request):
    return request.param


@pytest.fixture(params=get_files("wrong_field_datatypes"))
def wrong_field_datatypes_event(request):
    return request.param


@pytest.fixture(params=get_files("file_not_exists"))
def file_not_exists_event(request):
    return request.param


@pytest.fixture(params=get_files("file_empty"))
def file_empty_event(request):
    return request.param


@pytest.fixture(params=get_files("file_pk_violation"))
def file_pk_violation_event(request):
    return request.param


@pytest.fixture(params=get_files("swap_files"))
def swap_files_event(request):
    return request.param


@pytest.fixture()
def bad_lambda_event():
    return {}


@pytest.fixture()
def partner_bucket():
    return "coursera-degrees-data"


@pytest.fixture()
def conns():
    with mock_s3(), mock_ses():

        def boto_resource():
            res = boto3.client("s3",)
            client = boto3.client("ses", region_name="us-east-1")
            client.verify_email_identity(EmailAddress="datahub@coursera.org")

            settings_bucket = "coursera-data-engineering"
            upload_settings_test_files(res, settings_bucket)

            partner_bucket = "coursera-degrees-data"
            upload_partner_test_files(res, partner_bucket)

            logs_bucket = "coursera-data-engineering"
            upload_partner_test_files(res, logs_bucket)

            return (res, client)

        yield boto_resource


def upload_settings_test_files(res, bucket):
    folder = (
        "tests/test_datahub_degree_validator/" + "datahub/datahub_validator/settings/"
    )
    files = ["metadata.csv", "fieldnames.csv", "partner_schedule.csv"]
    upload_file(res, bucket, folder, files)


def upload_partner_test_files(res, bucket):
    folder = "tests/test_datahub_degree_validator/" + "test/degree/enrollments/"
    files = [
        "terms_20200124.csv",
        "terms_20200123.csv",
        "terms_20200125.csv",
        "terms_20200126.csv",
        "terms_20200127.csv",
        "terms_20200128.csv",
        "terms_20200129.csv",
        "degree_course_memberships_20200724.csv",
        "degree_term_memberships_20200805.csv",
        "degree_program_memberships_20200828.csv",
        "degree_term_courses_20200830.csv",
    ]
    upload_file(res, bucket, folder, files)


def upload_file(res, bucket, folder, files):
    res.create_bucket(Bucket=bucket)
    for file in files:
        file = folder + file

        res.upload_file(
            file, bucket, "/".join(file.split("/")[2:]),
        )


@pytest.fixture()
def log():
    log = OrderedDict(
        [
            ("error_code", 5),
            ("error_type", "PK Violation"),
            ("supplied_fields", ""),
            ("expected_fields", ""),
            ("no_supplied_fields", ""),
            ("no_expected_fields", ""),
            (
                "description",
                "\n\tDuplicates in Primary Key columns:\n\t\tstudent_id, admit_term_id: Total Duplicates: 4\n\t\tFor: 691262, 2310: Duplicates: 2\n\t\tFor: 1178592, 2260: Duplicates: 2\n\t\t...........",
            ),
            ("partner", "test"),
            ("program", "degree"),
            ("file_name", "degree_program_memberships_20200828.csv"),
            (
                "file_path",
                "test/degree/enrollments/degree_program_memberships_20200828.csv",
            ),
            ("file_no_of_rows", 11),
            ("log_file_name", "datahub_logs_test_degree_log_20200904.csv"),
            (
                "log_file_path",
                "datahub/datahub_validator/logs/test/degree/datahub_logs_test_degree_log_20200904.csv",
            ),
            ("priority", "URGENT"),
            ("partner_emails", ""),
            ("internal_emails", "piusnig@gmail.com;pmukiibi@coursera.org"),
            ("send_email", ""),
            ("date_time", datetime.datetime(2020, 9, 4, 21, 9, 32, 320637)),
        ]
    )
    return log
