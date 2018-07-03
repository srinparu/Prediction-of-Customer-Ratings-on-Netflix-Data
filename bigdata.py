%pyspark
from pyspark.sql.functions import mean, min, max
from pyspark.sql import SparkSession

def item(rdd,data1):
    if(data1>1000000):
        x=data1/1000000
        i=1000000
        j=0
        a1=[]
        resultant=[]
        #print (x)
        data2=rdd.zipWithIndex().filter(lambda vi: (vi[0]))
        #print (data2.take(10))
        for each in range(x+1):
            #print(each,j,i)
            data3=data2.filter(lambda (key,index) : j<= index < i ).keys()
            #print (data3.take(10))
            resultant=compute(data3,1000000)
            a1.append(resultant)
            j=i+1 
            i=i+1000000
        
        dataframe=createdataframe(a1)
        return dataframe
        
def compute(data,data1):
    a1=[]
    a2=[]
    a3=[]
    a4=[]
    a5=[]
    result1=data.map( lambda elem: elem)
    result2=result1.collect()
    
    for each in result2:
        for a in each:
            r=a.split(',')
           
            for b in r:
                k=b.split(" , ")
                #print (k)
                a1.append(k[0])
      
    for i in range (len(a1)):
        #for each in a1[i]:
        if(':' in a1[i]):
            b=a1[i]
            a2.append(a1[i])
            a3.append(i)
            a5.append([a1[i],[i]])
    
    
    i=0
    while (i<len(a3)):
        b=a3[i]
        c=a2[i]
        
        if(i==(len(a3)-1) and i<len(a3)):
            
            for b in range(b,len(a1),3):
                if((b!=(len(a1)-1)) and (b!=(len(a1)))):
                    a4.append([a1[b+1],a1[b+2],c[:-1]])
                    
        
        elif(i<len(a3)):
            for b in range(b,a3[i+1],3):
                a4.append([a1[b+1],a1[b+2],c[:-1]])
                #print (b,a3[i+1],"  ",a1[b+1],a1[b+2],c)
            
        i=i+1
            
    
    #print ("thi is",c[:-1])
    #print (a5) 
    #calmean(a5)
    return a4
   


def createdataframe(list):
    a=[]
    for each in list:
        a=a+each
    rdd2=sc.parallelize(a)
  
    df=rdd2.toDF(["id","rating","movieid"])
    return df
    
def perform(data):
    data1=data.count()
    a=data1/2
    print (data1/2)
    data2=data.zipWithIndex().filter(lambda vi: (vi[0]))
    k=a
    j=0
    list=[]
    list1=[]

    for i in range(2):
    
        data4=data2.filter(lambda (key,index) : j<= index < k ).keys()
        splitFile = data4.map(lambda x:x.split(' , '))
        listarray=item(splitFile,a)
        list.append(listarray)
        j=k+1
        k=k+a
    return list
               
data=sc.textFile("s3://bharatanenenu4/combined_data_1_1.txt")
data1=sc.textFile("s3://bharatanenenu4/combined_data_1_2.txt")
data2=sc.textFile("s3://bharatanenenu4/combined_data_2_1.txt")
data3=sc.textFile("s3://bharatanenenu4/combined_data_2_2.txt")
data4=sc.textFile("s3://bharatanenenu4/combined_data_3_1.txt")
data5=sc.textFile("s3://bharatanenenu4/combined_data_3_2.txt")
data6=sc.textFile("s3://bharatanenenu4/combined_data_4_1.txt")
data7=sc.textFile("s3://bharatanenenu4/combined_data_4_2.txt")
list=perform(data)
list1=perform(data1)
list2=perform(data2)
list3=perform(data3)
list4=perform(data4)
list5=perform(data5)
list6=perform(data6)
list7=perform(data7)
dataf=list[0].union(list[1])
dataf1=list1[0].union(list1[1])
dataf2=list2[0].union(list2[1])
dataf3=list3[0].union(list3[1])
dataf4=list4[0].union(list4[1])
dataf5=list5[0].union(list5[1])
dataf6=list6[0].union(list6[1])
dataf7=list7[0].union(list7[1])
datafr=dataf.union(dataf1)
datafr1=dataf2.union(dataf3)
datafr2=dataf4.union(dataf5)
datafr3=dataf6.union(dataf7)
frame1=datafr.union(datafr1)
frame2=datafr2.union(datafr3)
final=frame1.union(frame2)
#final.show()
#final.coalesce(1).write.csv('s3://bharatanenenu1/out1.csv') 
finaldf=final.select(final.id,final.rating.cast("float"),final.movieid.cast("int"))
#finaldf.show()   
#finaldf.repartition(1).write.option("header", "true").csv("s3://bharatanenenu4/mycsv.csv")

finaldf.createOrReplaceTempView("table")
print ("final",finaldf.count())


df=finaldf.groupby('movieid').count()
quant=df.approxQuantile("count",[0.8],0.0)
df.createOrReplaceTempView("movie")


df1=sqlContext.sql("SELECT movieid,count FROM movie WHERE count > 3821")
df1.show()
print (df1.count())
df1.createOrReplaceTempView("movieresult")


df2=sqlContext.sql("SELECT table.id,table.rating,movieresult.movieid,movieresult.count FROM movieresult,table WHERE movieresult.movieid=table.movieid ORDER BY movieresult.count asc")
df2.show()
print (df2.count())
df2.createOrReplaceTempView("resultant")


df3=finaldf.groupby('id').count()
quant1=df3.approxQuantile("count",[0.8],0.0)
df3.createOrReplaceTempView("idea")
print (quant1)

df4=sqlContext.sql("SELECT id,count FROM idea WHERE count > 79")
df4.show()
print (df4.count())
df4.createOrReplaceTempView("resagain")


df5=sqlContext.sql("SELECT resagain.id,resultant.rating,resultant.movieid,resultant.count FROM resagain,resultant WHERE resagain.id=resultant.id")
df5.show()
print (df5.count())




