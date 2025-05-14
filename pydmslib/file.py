
import sys
import uuid

import pyspark.sql.functions as f

from operator import itemgetter
from io import BytesIO, StringIO

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def read_parquet_files_and_add_input_file(base_dir, pattern=""):
    """
    Reads Parquet files from the specified directory and adds a new column indicating the source file name.

    Args:
        base_dir (str): The base directory where the Parquet files are stored.
        pattern (str, optional): An optional pattern to filter specific files. Defaults to an empty string.

    Returns:
        DataFrame: A PySpark DataFrame with an additional column 'inputFile' containing the source file name.
    """
    file_path = base_dir + pattern
    df = spark.read.parquet(file_path)
    return df.withColumn("inputFile", f.col("_metadata.file_name"))


def save_csv(spark_df, path, keep_whitespace=False, encoding="utf8"):
    """

    :param spark_df: Dataframe to save
    :param path: hdfs path
    :param keep_whitespace: Optional, if you want to keep leading and trailing whitespace on column names
    :param encoding: Optional, specify an encoding other than UTF8 (default)
    :return: None
    """

    whitespace_option = "true"
    if keep_whitespace:
        whitespace_option = "false"

    # get a temporary path for the local storage
    tmp_path = str(uuid.uuid4())

    # Run a cache then count to force the evaluation of the data frame into memory
    # N.B. http://apache-spark-developers-list.1001551.n3.nabble.com/Will-count-always-trigger-an-evaluation-of-each-row-td21018.html
    spark_df.cache().count()

    # write the dataframe as a single file to blob storage
    (spark_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option('encoding', encoding)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreLeadingWhiteSpace", whitespace_option)
        .option("ignoreTrailingWhiteSpace", whitespace_option)
        .format("com.databricks.spark.csv")
        .save(tmp_path))

    # Get the name of the wrangled-data CSV file that was just saved to Azure blob storage (it starts with 'part-')
    files = dbutils.fs.ls(tmp_path)
    output_file = [x for x in files if x.name.startswith("part-")]

    try:
        # Move the wrangled-data CSV file from a sub-folder (wrangled_data_folder) to mounted storage
        # While simultaneously changing the file name
        dbutils.fs.mv(output_file[0].path, path)
    except:
        raise IOError("File Save Failed: {}".format(sys.exc_info()))
    finally:
        # Finally, cleanup / delete the temporary directory
        dbutils.fs.rm(tmp_path, True)
        # Unpersist cached dataframe
        spark_df.unpersist()


def __istread_read_lines(istream):
    sc = spark.sparkContext
    reader = sc._gateway.jvm.java.io.BufferedReader(sc._jvm.java.io.InputStreamReader(istream))

    while True:
        this_line = reader.readLine()
        if this_line is not None:
            yield this_line
        else:
            break


def open_(path: str):
    """
    Opens a hdfs path and returns a BytesIO
    :param path: hdfs path
    :return: BytesIO or StringIO
    """
    # https://kb.databricks.com/python/hdfs-to-read-files.html
    # https://stackoverflow.com/questions/64756534/transforming-java-io-bufferedreader-into-python-object
    sc = spark.sparkContext
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = Path(path).getFileSystem(sc._jsc.hadoopConfiguration())
    istream = fs.open(Path(path))

    try:
        # Where possible read bytes
        return_object = BytesIO(istream.readAllBytes())
    except:
        # For older versions of java, readAllBytes is not a valid method
        # In these cases, return a StringIO
        return_object = StringIO("\n".join(__istread_read_lines(istream)))
    finally:
        istream.close()
    return return_object


def listdir(path: str):
    """
    Lists the names of all files in a hdfs directory (equivalent of os.listdir)
    :param path: hdfs path
    :return: list of strings
    """
    return [f.name for f in dbutils.fs.ls(path)]


def crawl(path: str, trim_size=None):
    """
    Lists all files under a directory
    :param path: hdfs path
    :param trim_size: How many characters to trim returned paths by (defaults to path length)
    :return: yields strings
    """
    if path[-1] != "/":
        path = path + "/"
    if trim_size is None:
        if path[:6] == "dbfs:/":
            trim_size = len(path)
        elif path[0] == "/":
            trim_size = 5 + len(path)
        else:
            trim_size = 6 + len(path)
    for f in dbutils.fs.ls(path):
        if f.path[-1] != "/":
            yield f.path[trim_size:]
        else:
            for g in crawl(f.path, trim_size):
                yield g


def exists(path):
    """

    :param path: hdfs path
    :return: If the supplied path exists (bool)
    """
    # https://forums.databricks.com/questions/20129/how-to-check-file-exists-in-databricks.html
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise


def find_latest_date_directory(root_dir):
    """
    Finds the latest directory a year/month/day/ partitioned structure
    :param root_dir: Directory to search
    :return: The latest directory by date
    """
    # Find latest year
    year_dir = root_dir + max(listdir(root_dir))
    # Month
    month_dir = year_dir + max(listdir(year_dir))
    # Day
    day_dir = month_dir + max(listdir(month_dir))

    return day_dir


def find_latest_date_partitioned(root_dir, split=None):
    """
    Finds the latest file alphabetically through a year/month/day/file partitioned structure
    :param root_dir: Directory to search
    :param split: Split the file names on a certain character and sort match after that
    :return: The latest file by date & alphabetically
    """
    # Find latest date directory
    day_dir = find_latest_date_directory(root_dir)

    # Find the target file
    if split is None:
        return day_dir + max(listdir(day_dir))
    else:
        f_names = listdir(day_dir)
        f_names_and_suffix = [(n, str(n).split(split)[-1]) for n in f_names]
        ans = max(f_names_and_suffix, key=itemgetter(1))[0]
        return day_dir + ans


def identify_to_process(metadata, process_dir, my_config=""):
    # Calculate files already processed
    if metadata is None:
        already_processed = set()
    else:
        if "config" not in metadata.columns:
            metadata = metadata.withColumn("config", f.lit(my_config))
        already_processed = set(
            [(x["file_name"], x["config"]) for x in metadata.select(f.col("file_name"), f.col("config")).collect()])

    # Check if grant directory exists
    if not exists(process_dir):
        if metadata is None:
            # Assume just not created yet and return empty
            return []
        else:
            # This could be a genuine issue, raise exception
            raise Exception("To Process Directory Not Found")

    # Get all files in the grant directory
    all_grant_files = list(crawl(process_dir))

    # Add config to all_grant_files
    all_grant_files = [(x, my_config) for x in all_grant_files]

    # Set difference to get all files to process
    files_to_process = list(set(all_grant_files) - already_processed)

    # Drop config names from files to process
    # Use a set as files will be processed more than once for different past configs
    files_to_process = list(set([x[0] for x in files_to_process]))

    # Done
    return files_to_process