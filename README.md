# pytest_dbconnect
Testing pyspark with pytest and Databricks Connect



## Setup Databricks Connect

https://docs.databricks.com/dev-tools/databricks-connect.html

Installing into a virtual environment

```powershell
# Create virtual environment (PowerShell)
py -3.7 -m venv .venv
.venv\Scripts\activate.ps1

pip uninstall pyspark
pip install -U databricks-connect==6.2.*
```



```
# Take note of:
URL=https://westeurope.azuredatabricks.net
TOKEN=<put token>
CLUSTER_ID=<put cluseter id>
ORG_ID=<put org id>
```



Configure:

```powershell
databricks-connect configure
```

After finished displays following information:

```
Updated configuration in C:\Users\ivang/.databricks-connect
* Spark jar dir: c:\sandbox\poc\databricksazuredevops\.venv\lib\site-packages\pyspark/jars
* Spark home: c:\sandbox\poc\databricksazuredevops\.venv\lib\site-packages\pyspark
* Run `pip install -U databricks-connect` to install updates
* Run `pyspark` to launch a Python shell
* Run `spark-shell` to launch a Scala shell
* Run `databricks-connect test` to test connectivity

```



Run sample pyspark to test

```powershell
databricks-connect test
```

### Issue

```
Service 'sparkDriver' could not bind on a random free port. You may check whether configuring an appropriate binding address
```

```
conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
sc = pyspark.SparkContext(master='local', appName='myAppName',conf=conf)

# export  SPARK_MASTER_IP=127.0.0.1 
export  SPARK_LOCAL_IP=127.0.0.1
```

#### Solution

```powershell
# $env:SPARK_MASTER_IP="127.0.0.1" 
$env:SPARK_LOCAL_IP="127.0.0.1"
databricks-connect test
```

### Issue 

Missing winutils

#### Solution

* Download from https://github.com/steveloughran/winutils
* Put in a directory `<path_to_hadoop>/bin/winutils.exe`
* Define environment variable:
  `HADOOP_HOME="<path_to_hadoop>"`

Note `winutils.exe` goes into the `bin` subdirectory.

### Issue

Error when running the code to obtain privileged token.

#### Solution

The code needs to be executed in a Scala cell

```scala
%scala
displayHTML(
  "<b>Privileged DBUtils token (expires in 48 hours): </b>" +
  dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>"))
```



## Running Tests

Install pytest (into the right virtual environment):

```bash
pip install pytest pytest-cov
# pandas is required by the helper function which 
# converts Spark DataFrame into list of dictionaries.
pip install pandas
```

Run the unit tests (bash)

```bash
pytest tests/unit
```

### With coverage

Coverage report in xml (for Azure DevOps) and html formats

```bash
pytest tests/unit --cov=app --cov-report=xml --cov-report=html
```

### With junit report

```bash
pytest tests/unit --cov=app --cov-report=xml --cov-report=html --junitxml=junit/test-results.xml
```



## Run in Jupyter

```powershell
.venv\Scripts\activate.ps1
pip install jupyter
# Used for %%sql
pip install autovizwidget
pip install PyArrow # Supress warning in %%sql
jupyter notebook
```



### Test Notebook

```python
import os
from pyspark.sql import SparkSession

os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

spark = SparkSession\
    .builder\
    .getOrCreate()

print("Let's sum the numbers from 0 to 100")
df = spark.range(101)

print(df.groupBy().sum('id').collect())
print(df)
```

```python
from IPython.core.magic import line_magic, line_cell_magic, Magics, magics_class

@magics_class
class DatabricksConnectMagics(Magics):

   @line_cell_magic
   def sql(self, line, cell=None):
       if cell and line:
           raise ValueError("Line must be empty for cell magic", line)
       try:
           from autovizwidget.widget.utils import display_dataframe
       except ImportError:
           print("Please run `pip install autovizwidget` to enable the visualization widget.")
           display_dataframe = lambda x: x
       return display_dataframe(self.get_spark().sql(cell or line).toPandas())

   def get_spark(self):
       user_ns = get_ipython().user_ns
       if "spark" in user_ns:
           return user_ns["spark"]
       else:
           from pyspark.sql import SparkSession
           user_ns["spark"] = SparkSession.builder.getOrCreate()
           return user_ns["spark"]

ip = get_ipython()
ip.register_magics(DatabricksConnectMagics)
```

```sql
%%sql
show databases
```





## Test Python Script

```python
import os
from pyspark.sql import SparkSession

# os.environ['SPARK_MASTER_IP'] = "127.0.0.1"
os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

spark = SparkSession\
    .builder\
    .getOrCreate()

print("Let's sum the numbers from 0 to 100")
df = spark.range(101)

print(df.groupBy().sum('id').collect())
```





## Python Launcher

```powershell
# Check where is python launcher (PowerShell)
Get-Command py

# Python launcher help
py --help

Python Launcher for Windows Version 3.8.150.1013

usage:
C:\Windows\py.exe [launcher-args] [python-args] script [script-args]

Launcher arguments:

-2     : Launch the latest Python 2.x version
-3     : Launch the latest Python 3.x version
-X.Y   : Launch the specified Python version
     The above all default to 64 bit if a matching 64 bit python is present.
-X.Y-32: Launch the specified 32bit Python version
-X-32  : Launch the latest 32bit Python X version
-X.Y-64: Launch the specified 64bit Python version
-X-64  : Launch the latest 64bit Python X version
-0  --list       : List the available pythons
-0p --list-paths : List with paths

The following help text is from Python:

usage: C:\Users\ivang\AppData\Local\Programs\Python\Python38\python.exe [option] ... [-c cmd | -m mod | file | -] [arg] ...
Options and arguments (and corresponding environment variables):
-b     : issue warnings about str(bytes_instance), str(bytearray_instance)
         and comparing bytes/bytearray with str. (-bb: issue errors)
-B     : don't write .pyc files on import; also PYTHONDONTWRITEBYTECODE=x
-c cmd : program passed in as string (terminates option list)
-d     : debug output from parser; also PYTHONDEBUG=x
-E     : ignore PYTHON* environment variables (such as PYTHONPATH)
-h     : print this help message and exit (also --help)
-i     : inspect interactively after running script; forces a prompt even
         if stdin does not appear to be a terminal; also PYTHONINSPECT=x
-I     : isolate Python from the user's environment (implies -E and -s)
-m mod : run library module as a script (terminates option list)
-O     : remove assert and __debug__-dependent statements; add .opt-1 before
         .pyc extension; also PYTHONOPTIMIZE=x
-OO    : do -O changes and also discard docstrings; add .opt-2 before
         .pyc extension
-q     : don't print version and copyright messages on interactive startup
-s     : don't add user site directory to sys.path; also PYTHONNOUSERSITE
-S     : don't imply 'import site' on initialization
-u     : force the stdout and stderr streams to be unbuffered;
         this option has no effect on stdin; also PYTHONUNBUFFERED=x
-v     : verbose (trace import statements); also PYTHONVERBOSE=x
         can be supplied multiple times to increase verbosity
-V     : print the Python version number and exit (also --version)
         when given twice, print more information about the build
-W arg : warning control; arg is action:message:category:module:lineno
         also PYTHONWARNINGS=arg
-x     : skip first line of source, allowing use of non-Unix forms of #!cmd
-X opt : set implementation-specific option
--check-hash-based-pycs always|default|never:
    control how Python invalidates hash-based .pyc files
file   : program read from script file
-      : program read from stdin (default; interactive mode if a tty)
arg ...: arguments passed to program in sys.argv[1:]

Other environment variables:
PYTHONSTARTUP: file executed on interactive startup (no default)
PYTHONPATH   : ';'-separated list of directories prefixed to the
               default module search path.  The result is sys.path.
PYTHONHOME   : alternate <prefix> directory (or <prefix>;<exec_prefix>).
               The default module search path uses <prefix>\python{major}{minor}.
PYTHONCASEOK : ignore case in 'import' statements (Windows).
PYTHONIOENCODING: Encoding[:errors] used for stdin/stdout/stderr.
PYTHONFAULTHANDLER: dump the Python traceback on fatal errors.
PYTHONHASHSEED: if this variable is set to 'random', a random value is used
   to seed the hashes of str and bytes objects.  It can also be set to an
   integer in the range [0,4294967295] to get hash values with a
   predictable seed.
PYTHONMALLOC: set the Python memory allocators and/or install debug hooks
   on Python memory allocators. Use PYTHONMALLOC=debug to install debug
   hooks.
PYTHONCOERCECLOCALE: if this variable is set to 0, it disables the locale
   coercion behavior. Use PYTHONCOERCECLOCALE=warn to request display of
   locale coercion and locale compatibility warnings on stderr.
PYTHONBREAKPOINT: if this variable is set to 0, it disables the default
   debugger. It can be set to the callable of your debugger of choice.
PYTHONDEVMODE: enable the development mode.
PYTHONPYCACHEPREFIX: root directory for bytecode cache (pyc) files.

```





## Resources

https://marketplace.visualstudio.com/items?itemName=riserrad.azdo-databricks&ssr=false#overview

https://docs.databricks.com/dev-tools/databricks-connect.html#ide-and-notebook-server-setup