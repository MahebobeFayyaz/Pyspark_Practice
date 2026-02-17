import os
import urllib.request
import ssl
from collections import namedtuple

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\DeLL\.jdks\corretto-1.8.0_482'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print()
"""
print("started my first code in pyspark")

a=2
b=3
print("Sum of a & b: ",a+b)

first_name="Fayyaz"
last_name = "Savanur"
print("Full Name: " +  first_name + " " + last_name)


### List
ls= [1,2,3,4,5,6]

print("List of integers: ", ls)
print("Length of list: ", len(ls))

rddls = sc.parallelize(ls)
print("RDD List: ",rddls.collect())

addrdd = rddls.map(lambda x: x+2)
print("addrdd(+2 to each element in list): ",addrdd.collect())

mulrdd =rddls.map(lambda x: x*10)
print("mulrdd(multiplied every element by 10): ", mulrdd.collect())

gt2rdd =rddls.filter(lambda x: x>2)
print("gt2rdd(element greater than 2): ", gt2rdd.collect())

print("================================================")
list_string= ["zeyo", "tera", "zeyobron"]
rddls_string =sc.parallelize(list_string)
print("RDD List: ",rddls_string.collect())

addrdd = rddls_string.map(lambda x: x+" Analytics")
print("addrdd(add Analytics to each element in list): ",addrdd.collect())

replacerdd = rddls_string.map(lambda x: x.replace("zeyo",""))
print("replacerdd(remove zeyo from each element in list): ",replacerdd.collect())

filterrdd = rddls_string.filter(lambda x: "zeyo" in x)
print("filterrdd(filter elements with zeyo): ",filterrdd.collect())

print("================================================")
list_string_2= ["A~B", "C~D"]
rddls_string_2 =sc.parallelize(list_string_2)
print("RDD List: ",rddls_string_2.collect())

print("Iterate every element of list and flatten every element with'~' delimiter")
flatrdd = rddls_string_2.flatMap(lambda x: x.split("~"))
print("flatrdd(flatten list): ",flatrdd.collect())

print()
print("==================================================================================================")
ls= ["state->TN~city->chennai","state->karnataka~city->bangalore"]
rdd_ls=sc.parallelize(ls)
print(rdd_ls.collect())

flatmap_rdd = rdd_ls.flatMap(lambda x : x.split("~"))
print(flatmap_rdd.collect())
state_rdd = flatmap_rdd.filter(lambda x: "state" in x)
print("State List: ", state_rdd.collect())
city_rdd = flatmap_rdd.filter(lambda x: "city" in x)
print("City List: ", city_rdd.collect())

State_rdd = state_rdd.map(lambda x: x.replace("state->",""))
print("State List: ", State_rdd.collect())
City_rdd = city_rdd.map(lambda x: x.replace("city->",""))
print("City List: ", City_rdd.collect())

print()
print("=====================================================================")
print("File read from State.txt")
filerdd= sc.textFile("state.txt")
print("State.txt content: ",filerdd.collect())

flatmap_filerdd = filerdd.flatMap(lambda x: x.split('~'))
print()
flatmap_filerdd.foreach(print)

state_filerdd = flatmap_filerdd.filter(lambda x: "State" in x)
print("========State=================")
state_filerdd.foreach(print)

city_filerdd = flatmap_filerdd.filter(lambda x: "City" in x)
print("==============City=================")
city_filerdd.foreach(print)

state_dd = state_filerdd.map(lambda x: x.replace("State->",""))
print("========State=================")
state_dd.foreach(print)

city_rdd = city_filerdd.map(lambda x: x.replace("City->",""))
print("==============City=================")
city_rdd.foreach(print)


print()
print("===================================================================")
print("usdata.csv")
print("===================================================================")

usdata = sc.textFile('usdata.csv')
print('==============Raw Data============')
#usdata.foreach(print)

len_usdata = usdata.filter(lambda x: len(x)>200)
print('==============data with row length >200============')
len_usdata.foreach(print)

flat_usdata = len_usdata.flatMap(lambda x: x.split(','))
print('==============Flat Data with ',' delimiter============')
flat_usdata.foreach(print)

replaced_usdata = flat_usdata.map(lambda x: x.replace('-',''))
print('==============Data replaced "-" with ''============')
replaced_usdata.foreach(print)

zeyo_usdata = replaced_usdata.map(lambda x: x + ", Zeyo")
print('==============Add Zeyo for each element============')
zeyo_usdata.foreach(print)

print('==============Write results to a directory============')
#zeyo_usdata.saveAsTextFile("file:///E:/rddout")
print("=======Succesfully Saved The file")

print()
print("============================================")
print("==========dt.txt============================")
print("============================================")

data = sc.textFile("dt.txt")
data.foreach(print)

filter_data = data.filter(lambda x: "Gymnastics" in x)
print("dt.txt rows with Gymnastics")
filter_data.foreach(print)

print("Column Based Processing")
print("Iterate every row in dt.txt and filter 5th columns contains Gymnastics having ',' as a delimiter")
#Step 1: Read the data
data = sc.textFile("dt.txt")
data.foreach(print)
# Step 2: Split data ',' delimiter
split_data = data.map(lambda x : x.split(','))
#Step 3: define columns using namedtuple
from collections import namedtuple
Columns = namedtuple(typename='Columns',
    field_names=['Id','tDate','Amount', 'Category','Product','Mode']
)
#Step 4: Impose columns to data splits using index
final_data = split_data.map(lambda x: Columns(x[0],x[1],x[2],x[3],x[4],x[5]))
print()
print("===========Final Columnar Data========================")
final_data.foreach(print)
#Step 5: perform column filters
prod_data = final_data.filter(lambda x: 'Gymnastics' in x.Product)
print("==================Filtered Data=========================")
prod_data.foreach(print)
#Step 6 Write the filtered data as a parquet file
""""""
RDD do not support parquet file, Dataframe does.
Convert RDD to Dataframe using toDF()
""""""
df = prod_data.toDF()
df.show()
## Write Dataframe to parquet file
#df.write.parquet("file///E:/parquetout")

# Read Data directly as a  Dataframe
# CSV File
print("================CSV==============")
csv_df =(
    spark
    .read
    .format('csv')
    .option(key='header',value='True')
    .load('usdata.csv')
)

csv_df.show()

#json File
print("================JSON==============")
json_df =(
    spark
    .read
    .format('json')
    #.option(key='header',value='True')
    .load('file4.json')
)

json_df.show()

#parquet File
print("================Parquet==============")
parquet_df =(
    spark
    .read
    .format('parquet')
    #.option(key='header',value='True')
    .load('file5.parquet')
)

parquet_df.show()

#orc File
print("================ORC==============")
orc_df =(
    spark
    .read
    .format('orc')
    #.option(key='header',value='True')
    .load('data.orc')
)

orc_df.show()

#Give a name to dataframe and perform SQL queries using spark.sql("<sql query>").show()
orc_df.createOrReplaceTempView("orc_sql")
spark.sql("select * from orc_sql where id=0").show()


csv_df.createOrReplaceTempView('ustable')
print("SQL Output")
spark.sql("select * from ustable where state='LA'").show()

#DSL (Domain Specific Language)
print("DSL Output")
LA_df = csv_df.filter("state='LA'")
LA_df.show()


#Step 1: Read the data
data = sc.textFile("dt.txt")

# Step 2: Split data ',' delimiter
split_data = data.map(lambda x : x.split(','))
#Step 3: define columns using namedtuple
from collections import namedtuple
Columns = namedtuple(typename='Columns',
                     field_names=['Id','tDate','Amount', 'Category','Product','Mode']
                     )
#Step 4: Impose columns to data splits using index
final_data = split_data.map(lambda x: Columns(x[0],x[1],x[2],x[3],x[4],x[5]))
data= final_data.toDF()
data.show()

df1 = data.select('tDate','Amount')
df1.show()
df2 = data.drop('tDate','Amount')
df2.show()

#Single column filter
df3 = data.filter("Category = 'Exercise'")

# Multi column filter
df4 = data.filter("Category = 'Exercise' and Mode = 'cash'")
df4.show()

df5 =data.filter("Category = 'Exercise' or Mode= 'cas'")
df5.show()


#Multi Value Filter
df6 =data.filter("Category in ('Exercise','Gymnastics')")
df6.show()
"""

"""
# SQL GET READY CODE

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()




data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()






data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()

df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")


print('SQL Get Ready code completed')


spark.sql("select * from df").show()

# select only two columns
print("=============================")
print("Select id,tdate from df")
spark.sql("select id,tdate from df").show()

#Single column filter
#filter data to select category = Exercise
print("===============================")
print("Single column filter")
spark.sql("select * from df where category = 'Exercise'").show()

#Multi Column filter
print("============================================")
print("Multi column filter")
spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby = 'cash'").show()

#Multi Value filter
print("==================================")
print("Multi Value filter")
spark.sql("select * from df where category in('Exercise','Gymnastics')").show()

#LIKE filter
print("==================================")
print("LIKE Operator")
spark.sql("select * from df where product like '%Gymnastics%'").show()

#Not equals filter
print("==================================")
print("Not equals Operator")
spark.sql("select * from df where category != 'Exercise'").show()

#NOT IN filter
print("==================================")
print("NOT in Operator")
spark.sql("select * from df where category not in('Exercise','Gymnastics')").show()

#NULL filter
print("==================================")
print("is null")
spark.sql("select * from df where product is null").show()

#not NULL filter
print("==================================")
print("is not null")
spark.sql("select * from df where product is not null").show()

#MAX function
print("==================================")
print("MAX function")
spark.sql("select max(id) from df ").show()

#MAX function
print("==================================")
print("MAX function")
spark.sql("select max(id) from df ").show()

#MIN function
print("==================================")
print("MIN function")
spark.sql("select min(id) from df ").show()

#count function
print("==================================")
print("count function")
spark.sql("select count(*) as Total from df ").show()

# Case When
print("==================================")
print(" Case When")
spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as Status from df").show()

# Concat two columns
print("==================================")
print("Concat two columns")
spark.sql("select id, category, concat(id,'-',category) as `ID + Category` from df").show()

#concat multiple columns
print("==================================")
print("Concat multiple columns")
spark.sql("select id, category, product, concat_ws('-',id,category,product) as `ID + Category + Product` from df").show()

#lowercase
print("==================================")
print("lowercase")
spark.sql("select category,lower(category) as Lower from df ").show()

#Uppercase
print("==================================")
print("uppercase")
spark.sql("select category,upper(category) as Upper from df ").show()

#CEIL
print("==================================")
print("CEIL")
spark.sql("select amount,ceil(amount) as CEIL from df ").show()

#round
print("==================================")
print("round")
spark.sql("select amount,round(amount) as Round from df ").show()

#coalesce: Replace Nulls
print("==================================")
print("coalesce")
spark.sql("select product, coalesce(product,'NA') from df ").show()

#trim the space
print("==================================")
print("trim the space")
spark.sql("select product, trim(product) from df ").show()

#distinct
print("==================================")
print("Distinct")
spark.sql("select distinct(category) from df ").show()

#substring
print("==================================")
print("substring")
spark.sql("select product, substring(product,1,10) from df ").show()

#substring with split
print("==================================")
print("substring with split")
spark.sql("select product, split(product,' ')[0] from df ").show()

#Union All
print("==================================")
print("Union All")
spark.sql("select * from df union all select * from df1 ").show()

#Union
print("==================================")
print("Union ")
spark.sql("select * from df union select * from df1 ").show()

#group by: Total amount collected for each category
print("==================================")
print("groupby")
spark.sql("select category,sum(amount) as Total from df group by category").show()

#group by on two columns and order by
print("==================================")
print("group by and order by on two columns")
spark.sql("select category,spendby,sum(amount) as Total from df group by category,spendby order by category").show()

#group by, count and order by
print("==================================")
print("group by on two columns and count")
spark.sql("select category,spendby,sum(amount) as Total ,count(amount)from df group by category,spendby order by category").show()


#max of every category
print("==================================")
print("max of every category")
spark.sql("select category,max(amount) as max from df group by category").show()

#Window functions
#window row number
print("==================================")
print("window row number")
spark.sql("select category,amount, row_number() "
          "OVER(partition by category order by amount desc ) as row_number "
          "from df").show()

#Rank
print("==================================")
print("Rank")
spark.sql("select category, amount, rank() "
          "OVER(partition by category order by amount desc) as rank "
          "from df").show()

#Dense Rank
print("==================================")
print("Dense Rank")
spark.sql("select category, amount, dense_rank() "
          "OVER(partition by category order by amount desc) as dense_rank "
          "from df").show()

#Lead
print("==================================")
print("Lead")
spark.sql("select category, amount, lead(amount) "
          "OVER(partition by category order by amount desc) as lead "
          "from df").show()

#Lag
print("==================================")
print("Lag")
spark.sql("select category, amount, lag(amount) "
          "OVER(partition by category order by amount desc) as lag "
          "from df").show()

#having
print("==================================")
print("having")
spark.sql("select category, count(category) as count "
          "from df "
          "group by category "
          "having count >2").show()

#JOINS
#inner join
print("==================================")
print("inner Join")
spark.sql("select cust.*, prod.product from cust "
          "inner join prod "
          "on cust.id = prod.id").show()

#left join
print("==================================")
print("left Join")
spark.sql("select cust.*, prod.product from cust "
          "left join prod "
          "on cust.id = prod.id").show()

#right join
print("==================================")
print("right Join")
spark.sql("select cust.*, prod.product from cust "
          "right join prod "
          "on cust.id = prod.id").show()

#full join
print("==================================")
print("Full Join")
spark.sql("select cust.*, prod.product from cust "
          "full join prod "
          "on cust.id = prod.id").show()

#Left Anti Join
print("==================================")
print("Left Anti Join")
spark.sql("select cust.* from cust "
          "left anti join prod "
          "on cust.id = prod.id").show()

#Date Format
print("==================================")
print("Date Format")
spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') "
          "as con_date from df").show()
"""
#DSL (Domain Specific Language)
#Step 1: Read the data
data = sc.textFile("dt.txt")

# Step 2: Split data ',' delimiter
split_data = data.map(lambda x : x.split(','))
#Step 3: define columns using namedtuple
from collections import namedtuple
Columns = namedtuple(typename='Columns',
                     field_names=['Id','tDate','Amount', 'Category','Product','Mode']
                     )
#Step 4: Impose columns to data splits using index
final_data = split_data.map(lambda x: Columns(x[0],x[1],x[2],x[3],x[4],x[5]))
data= final_data.toDF()
data.show()
"""
df1 = data.select('tDate','Amount')
df1.show()
df2 = data.drop('tDate','Amount')
df2.show()

#Single column filter
df3 = data.filter("Category = 'Exercise'")

# Multi column filter
df4 = data.filter("Category = 'Exercise' and Mode = 'cash'")
df4.show()

df5 =data.filter("Category = 'Exercise' or Mode= 'cas'")
df5.show()


#Multi Value Filter
df6 =data.filter("Category in ('Exercise','Gymnastics')")
df6.show()

#LIKE
df7 = data.filter("Product like '%Gymnastics%'")
df7.show()

#is null
df8 =data.filter("Product is null")
df8.show()

#is not null
df9 =data.filter("Product is not null")
df9.show()

# not equal
df9 =data.filter("Category != 'Exercise'")
df9.show()


#Upper
df10 =data.selectExpr('Category','upper(Category)')
df10.show()
"""
# Expressions
df10 =data.selectExpr(
    'cast(Id as int) as Cast_Id',
    'split(tDate,"-")[2] as Year',
    'Amount + 1000',
    'upper(Category) as CATEGORY',
    'concat(Product,"~Zeyo")',
    'Mode',
    'case when Mode="cash" then 1 else 0 end as Status'
)
df10.show()























