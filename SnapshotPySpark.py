import pyspark
from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from _datetime import date
from typing import Final
from pyspark import SparkContext , SparkConf
from pyspark.sql import Window
import sys
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Debtors Task").getOrCreate()

Snapshot = spark.read.csv(path = r"C:\Users\AnmolPal\Desktop\Docs\Desktop/SnapShot.csv",sep = ',',header = True,quote = "")

Snapshot = Snapshot.withColumn("Date", to_date("Date", 'dd-MM-yyyy'))

Snapshot = Snapshot.withColumn("net_qty", col("net_qty").cast(IntegerType()))
Snapshot = Snapshot.withColumn("lot_code", col("lot_code").cast(IntegerType()))
Snapshot = Snapshot.withColumn("Branch_Code", col("Branch_Code").cast(IntegerType()))

# Snapshot = Snapshot.withColumn('Date', col('Date').cast(DateType()))

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
Snapshot =Snapshot.withColumn('Date',to_date(unix_timestamp('Date', 'dd-MM-yyyy').cast('timestamp')))

# Snapshot.printSchema()
# Snapshot.show()

min_date = Snapshot.select(min('Date'))
min_date = min_date.withColumn('Today', current_date())
min_date = min_date.withColumnRenamed("min(Date)","minDate")
# min_date.printSchema()
# min_date = min_date.withColumn('bookingDt', col('bookingDt').cast('date')).withColumn('arrivalDt', col('arrivalDt').cast('date'))
#
Date = min_date.withColumn('All Dates', explode(expr('sequence(minDate, Today, interval 1 day)')))

Date = Date.drop('minDate', 'Today')

Date = Date.withColumn('Key',lit(1))

# Date.show()

Data = Snapshot.groupby(['Branch_Code','lot_code']).agg(sum('net_qty').alias('net_qty'),min('Date').alias('MinDate'),max('Date').alias('MaxDate'))
# Data = Data.withColumn('MinDate', Snapshot.groupby(['Branch_Code','lot_code']).min('Date'))

Data = Data.withColumn('Key',lit(1))
# Snapshot.printSchema()
# Snapshot.show()
#
Joined_Data = Date.join(Data, Date.Key == Data.Key)

Joined_Data = Joined_Data.drop('Key')


# print((Joined_Data.count(), len(Joined_Data.columns)))

Joined_Data = Joined_Data.withColumnRenamed("All Dates","All_Dates")

Joined_Data = Joined_Data.withColumn('ConKey', concat(col('All_Dates'),col('Branch_Code'),col('lot_code')))

Joined_Data = Joined_Data.withColumnRenamed("ConKey","Con_Key")

# Joined_Data = Joined_Data.withColumn('ConKey', col('ConKey').cast(IntegerType()))

# Joined_Data.show()

# Joined_Data.printSchema()

Filter = Joined_Data.filter(Joined_Data.MinDate <= Joined_Data.All_Dates)
# Filter = Joined_Data.withColumn("Flag",expr("case when MinDate <= All Dates then 0"))

# Filter.show()

# Final = Filter.withColumn('Flag', when(col('MinDate')>col('All_Dates'), 0).when(col('MaxDate')<col('All_Dates') & col('net_qty')==0,0).otherwise(1))

Final = Filter.withColumn('Flag', when(Filter['MinDate']>Filter['All_Dates'], 0).when((Filter['MaxDate']<Filter['All_Dates']) & (Filter['net_qty'] == 0),0).otherwise(1))

Final = Final.filter(Final.Flag == 1)

# Final.show()

Final = Final.withColumn('ConKey', concat(col('All_Dates'),col('Branch_Code'),col('lot_code')))

Final = Final.withColumnRenamed("ConKey","Con_Key")

Final = Final.drop('lot_code', 'Branch_Code', 'Flag')


Final = Final.withColumnRenamed("MinDate","Min_Date")
Final = Final.withColumnRenamed("MaxDate","Max_Date")
Final = Final.withColumnRenamed("All_Dates","AllDates")
# Final.show(500)

output = Final.join(Joined_Data, on = 'Con_Key', how ='left')
#
output = output.drop('AllDates','Min_Date', 'Max_Date')

# output.show(500)

# output = output.filter(output.lot_code == 4743770)

# output.show(500)

output = output.withColumnRenamed("Branch_Code","Branch Code")
output = output.withColumnRenamed("lot_code","lot code")
output = output.withColumnRenamed("net_qty","net qty")

Final2 = Snapshot

Final2 = Final2.withColumn('Con_Key', concat(col('Date'),col('Branch_Code'),col('lot_code')))

# Final2.show()

Final3 = output.join(Final2, on= 'Con_Key', how= 'left')

# Final3 = Final3.filter(Final3.lot_code == 4499295)

Final3 = Final3.drop('net qty', 'Branch_Code', 'Date', 'lot_code','Con_Key')

Final3 = Final3.fillna({'net_qty' : 0})

# Final3.show()

Final3 = Final3.withColumn("Cumsum",F.sum("net_qty").over(Window.partitionBy("lot code").rowsBetween(-sys.maxsize, 0)))
Final3 = Final3.drop('MinDate', 'MaxDate', 'Input', 'net_qty')
# Final3 = Final3.withColumn("Cumsum", Final3["Cumsum"] )
Final3 = Final3.withColumnRenamed("All_Dates", "Date")

Final3.show(1000)
print(Final3.count())


# Final3.write.format('csv').option('header',True).save('SnapShot.csv')
