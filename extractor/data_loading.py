# Implementing Custom DataExtractor, ParallelExecutor and Combiner
import os
import sys
import time
import logging
import warnings
import oracledb
import pandas as pd
from joblib import load
from getpass import getpass
from concurrent.futures import as_completed
from oracledb import IntegrityError as Error
from concurrent.futures import ThreadPoolExecutor

      
# DataExtractor class
class DataExtractor(object):
    """
        A class used for extraction of data and saving the data in different file formats
    
        such as csv, excel, parquet, sql etc, it can also be used for fetching the data
    
        and can also b parallelized using the ParallelExecutor class which can run multiple
    
        DataExtractor objects at time with multithreading across cpu cores.
        
        
        Attributes
        ----------
        
        sql_file: str
            a sql file path where the sql file is stored in string format.
            
        format_type: str
            the format_type specifies which file format the DataExtractor should save.
            
        server: str
            the server attribute specifies on which server the DataExtractor should run
            .
        tag: str
            the tag attribute is just a file descripter for sql file.
            
        ind: bool
            the ind attribute is just for printing the runtime on to the console.
        
        
        Methods:
        --------
        
        extract(SAVE_PATH: str) -> None:
            takes the input for save path for sql file and converts the sql file into preferred file formats.
            
            
        get_data() -> pd.DataFrame:
            fetches the dataframe after the sql file has been successfully extracted by the DataExtractor
    """
    def __init__(
                     self,
                     sql_file: str, 
                     format_type: str="parquet", 
                     server: str="datascience",
                     tag: str="",
                     ind: bool=True
                ) -> None:
        
        """This is DataExtractor class constructor"""      
        self.sql_file = sql_file
        self.format_type = format_type
        self.server = server
        self.tag = tag
        self.ind = ind
    
    def extract(self, SAVE_PATH: str) -> None:
        """
            Method Name: extract
            Description: This method extracts the sql file and saves it to the specified format_type
            Parameters: SAVED_PATH in string format
            Return Type: None
            
            >>> de.extract("/mnt/datascience/codes/Rohan/ParquetFiles/Mart/EXAMPLE.parquet")
        """
        
        try:
            # Ignoring Warnings
            warnings.simplefilter("ignore")
            
            format_list = ["parquet", "csv", "excel"]
            if self.format_type not in format_list:
                raise ValueError(f"The file format {self.format_type} mentioned is not valid!")
                
            # Reading the config file
            configs = pd.read_parquet("C:\\Users\\cherukuri.rohan\\Desktop\\extractor\\config.parquet", engine="pyarrow")
            idx = 0 # Where idx can take only one value (0 or 1 or 2 or 3 or 4)
            if self.server.upper() == "DATASCIENCE":
                idx = 0
            elif self.server.upper() == "BI_DS":
                idx = 1
            elif self.server.upper() == "CRM_BI":
                idx = 2
            elif self.server.upper() == "BI_FIU":
                idx = 3
            elif self.server.upper() == "BI_DA":
                idx = 4
            else:
                raise ValueError(f"ServerError: {self.server} no such server found for extracting the data!")
            self.con = oracledb.connect(
                                            user=configs.iloc[idx]["username"], 
                                            password=configs.iloc[idx]["password"], 
                                            host=configs.iloc[idx]["host"],
                                            port=configs.iloc[idx]["port"],
                                            service_name=configs.iloc[idx]["database"]  
                                        )
            
            # Tagging the Sql files
            if self.tag == "" and self.ind == True:
                fileName = self.sql_file.split("/")[-1]
                self.tag = fileName[:len(fileName) - 4]
            print(f"Extracting the {self.tag} data...")
            # Reading the sql file
            with open(self.sql_file, "r") as file:
                full_sql = file.read()
                
            # Calculating run-time of the DataExtractor class
            t1 = time.perf_counter()
            output_file = pd.read_sql(full_sql, con=self.con)

            if self.format_type == "parquet":
                # Writing the extracted file to parquet file 
                output_file.to_parquet(SAVE_PATH, use_deprecated_int96_timestamps=True, compression="gzip")
                
            elif self.format_type == "csv":
                # Writing the extracted file to csv file
                output_file.to_csv(SAVE_PATH)
            
            elif self.format_type == "excel":
                # Writing the extracted file to excel file
                output_file.to_excel(SAVE_PATH)
            
            else:
                print(f"{self.format_type} is not a correct file extension!")
            
            t2 = time.perf_counter()
            # Saving the file path for later usage
            self.path = SAVE_PATH
            self.run_time = round(t2 - t1, 3)
            # Printing the message on to the console
            self.status_code = "Successfull"
            print(f"[Finished extracting {self.tag} data in: {self.run_time} sec(s), with status_code: {self.status_code}]")
        except (KeyboardInterrupt, Exception, Error) as e:
            self.status_code = "Failed"
            print("Error occured inside the extract method from the DataExtractor class " + str(e))
            print(f"Extraction of the {self.tag} data from the sql file failed!, exitted with status_code {self.status_code}")

    
    def get_data(self) -> pd.DataFrame:
        """
            Method Name: get_data
            Description: This method returns the parquet file
            Parameters: SAVED_PATH in string format
            Return Type: pd.DataFrame
            
            >>> df = de.get_data()
        """
        try:
            df = pd.DataFrame()
            # Reading the parquet file
            if self.format_type == "parquet":
                df = pd.read_parquet(self.path, engine="pyarrow")
            # Reading the csv file   
            elif self.format_type == "csv":
                df = pd.read_csv(self.path)
            # Reading the excel file 
            else:
                df = pd.read_excel(self.path, engine="openpyxl")
            return df
        except (KeyboardInterrupt, Exception, Error) as e:
            print("Error occured inside the get_data method from the DataExtractor class " + str(e))
            print("Failed to fetch the data!")
    
    

         
    def __repr__(self) -> str:
        """This is a special method used for representation of DataExtractor class"""
        try:
            return f"DataExtractor(sql_file={self.sql_file}, format_type={self.format_type}, server={self.server}, ind={self.ind})"
        except Exception as e:
            print("Error occured inside the __repr__ method from the DataExtractor class " + str(e))

            
            
# ParallelExecutor class
class ParallelExecutor(object):
    """
        This is a class implementation for executing multiple DataExtractor objects parallely by using multi-threading.
        
        Attributes
        ----------
        
        data_list: list[DataExtractor]
            the data_list attribute contains the list of DataExtractor objects which is of type DataExtractor.
        
        path_list: list[str]
            the path_list attribute contains the list of output file paths in string format.
        
        n_workers: int
            the n_workers attribute converts the DataExtractor into threads and run's it in cpu cores
            usually the number of queries corresponds to the number passed to n_workers attribute.
                       
        
        Methods
        -------
        
        execute() -> None:
            this method executes all the DataExtactor objects parallely with multi-threading conept.
    """
    
    """
        Example:- 
        >>> from data_loading import DataExtractor, ParallelExecutor
        >>> de1 = DataExtractor(sql_file="path1", format_type="parquet", server="datascience")
        >>> de2 = DataExtractor(sql_file="path2", format_type="parquet", server="datascience")
        >>> de3 = DataExtractor(sql_file="path3", format_type="parquet", server="datascience")
        >>> de = [de1, de2, de3]
        >>> paths = ["to_path1", "to_path2", "to_path3"]
        >>> ParallelExecutor(de, paths, n_workers=3).execute()
        >>> 'Usually the n_workers will be number of DataExtractor objects present in the list'
    """
    def __init__(self, data_list: list[DataExtractor], path_list: list[str], n_workers: int=2) -> None:
        """This is a ParallelExecutor constructor"""
        self.data_list = data_list
        self.path_list = path_list
        self.n_workers = n_workers
    
    
    def execute(self) -> None:
        """
            Method Name: execute 
            Description: This method takes DataExtractor objects as input and run's them concurrently
            Parameters: None
            Return Type: None
            
            >>> 'NOTE:- Here the n_workers creates 'n' threads which is not equal to the number of cpu cores.' 
            >>> 'Here you can configure the n_workers argument depending on the number of tasks you want to execute.'
        """
        try:
            # Setting conditions 
            if len(self.data_list) == len(self.path_list):
                    with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                        futures = [executor.submit(de.extract, path) for de, path in dict(zip(self.data_list, self.path_list)).items()]
                        for future in as_completed(futures):
                            output = future.result()
            else:
                raise ValueError(f"Length mismatch of the lists {self.data_list} and {self.path_list}")
        except (KeyboardInterrupt, Exception) as e:
            print("Error occured inside the execute method of ParallelExecutor class " + str(e))
            
    
    def __repr__(self) -> str:
        """This is a special method used for representation of ParallelExecutor class"""
        try:
            return f"ParallelExecutor(n_workers={self.n_workers})"
        except (KeyboardInterrupt, Exception) as e:
            print("Error occured inside the __repr__ method from the ParallelExecutor class " + str(e))

            

# Combiner class
class Combiner(object):
    """
        This is a class implementation for executing multiple DataExtractor objects and ParallelExecutor object  parallely.
        
        Attributes
        ----------
        sql_files: list[str]
            the sql_files are a list of string paths for reading the Sql files.
        
        paths: list[str]
            the path_list attribute contains the list of output file paths in string format.
        
        format_type: str
            the format_type attribute write's the output file in a particular format.
            
        server: str
            the server attribute get's executed in a particular server.
        
        password: bool
            the password attribute is used for encrypting the Combiner class.
            
            
        staticmethod:    
        -----------
        get_cpu_cores() -> int:
            the get_cpu_cores is a class method where it returns the number of cpu cores prsent in the machine.
        

        
        Methods
        -------
        
        validate() -> None:
            this method checks if the password entered is correct or not.
        
        execute() -> None:
            this method executes all the DataExtactor objects parallely with multi-threading concept using ParallelExecutor class.
            
        collect() -> list[pd.DataFrame]:
            this method collects and retuns all the  list dataframe's.
    """
    
    """
        Example:- 
        >>> from data_loading Combiner
        >>> sql_files = ["sql_file1", "sql_file2", "sql_file3"]
        >>> paths = ["to_path1", "to_path2", "to_path3"]
        >>> dataframes = Combiner(sql_files, paths, n_workers=3, format_type="parquet", server="bistar_read").execute().collect()
        >>> 'Usually the n_workers will be number of DataExtractor objects present in the list'
        >>> 'Note: This Combiner class can only be used if the format_type and server are same for all sql_files'
    """
    def __init__(self, sql_files: list[str]=[], 
                 paths: list[str]=[],
                 format_type: str="parquet",
                 server: str="datascience",
                 password: bool=True
                ) -> None:
        """This is a Combiner class constructor"""
        self.password = password
        # Keeping the Combiner class password protected
        self.validate()
        self.sql_files = sql_files
        self.de = []
        self.paths = paths
        self.format_type = format_type
        self.server = server
        
        
    
    # Helper method
    def validate(self) -> None:
        """
            Method Name: validate 
            Description: This method validates the password
            Parameters: None
            Return Type: None
        """
        try:
            if self.password == False:
                pass
            else:
                passwd = getpass("Enter the password: ")
                key = load("C:\\Users\\cherukuri.rohan\\Desktop\\extractor\\key.pkl")
                if passwd == key["password"]:
                    print("Successfully logged in!")
                else:
                    print("Login failed due to incorrect password!")
                    sys.exit()
        except Exception as e:
            print("Error occured inside the Combiner class " + str(e))
            sys.exit()
    
    @staticmethod
    def get_cpu_cores() -> int:
        """This method return's the cpu cores present in the machine"""
        # Getting number of cores in a machine
        return os.cpu_count()
    
    
    def execute(self) -> None:
        """
            Method Name: execute 
            Description: This method takes DataExtractor objects as input and run's them concurrently
            Parameters: None
            Return Type: None
            
            >>> 'NOTE:- Here the n_workers creates 'n' threads which is not equal to the number of cpu cores.' 
            >>> 'Here you can configure the n_workers argument depending on the number of tasks you want to execute.'
        """
        try:
            if (self.sql_files == []) and (self.paths == []):
                return self
            if len(self.sql_files) == len(self.paths):
                if len(self.sql_files) == 1:
                    self.de = DataExtractor(self.sql_files[0], format_type=self.format_type, server=self.server)
                    self.de.extract(self.paths[0])
                else:
                    self.de = [DataExtractor(sql_file=sql_file, format_type=self.format_type, server=self.server) for sql_file in self.sql_files]
                    ParallelExecutor(self.de, self.paths, n_workers=len(self.sql_files)).execute()
                return self
            else:
                raise ValueError(f"Length mismatch of the lists {self.sql_files}, {self.de} and {self.paths}")
        except (KeyboardInterrupt, Exception) as e:
            print("Error occured inside the execute method of Combiner class " + str(e))
            sys.exit()
              
    def collect(self) -> list[pd.DataFrame]:
        """
            Method Name: collect 
            Description: This method fetches the dataframes
            Parameters: None
            Return Type: list[pd.DataFrame]
            
            >>> 'NOTE:- Here the n_workers creates 'n' threads which is not equal to the number of cpu cores.' 
            >>> 'Here you can configure the n_workers argument depending on the number of tasks you want to execute.'
        """
        try:
            if self is None:
                print("The sql_files and paths are empty!")
            else:
                if len(self.sql_files) == 1:
                    return [self.de.get_data()]
                else:
                    return [de.get_data() for de in self.de]
        except (KeyboardInterrupt, Exception) as e:
            print("Error occured inside the collect method of Combiner class " + str(e))
    
                  
    def __repr__(self) -> str:
        """This is a special method used for representation of the Combiner class"""
        try:
            return f"Combiner(sql_files={self.sql_files}, paths={self.paths}, format_type={self.format_type}, server={self.server})"
        except (KeyboardInterrupt, Exception) as e:
            print("Error occured inside the __repr__ method from the Combiner class " + str(e))        