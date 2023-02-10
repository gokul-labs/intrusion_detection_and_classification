from kafka import KafkaProducer
import time
import os,sys
import random
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
inputrate=1

def send_at(rate,input):
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'cyberproduce'
    interval = 1/rate
    counter=1
    bias=1
    t1=time.time()
    t2=0
    for f in os.listdir(input):
        with open(os.path.join(input,f)) as fil:
            temp=0
            for line in fil:
                interval = 1/rate
                rate=rate-temp
                t2=time.time()
                if(t2-t1>10):
                    bias+=1
                    t1=t2
                line+=","+str(bias)
                producer.send(topic,line.encode('ascii'))
                if(rate<1):
                    rate=random.randrange(1,2)
                    print("rate :",rate)
                elif(counter<30):
                    rate=random.randrange(1,5)
                    print("rate :",rate)
                elif(counter<50):
                    rate+=(random.randrange(1,2)/100)
                    print("rate :",rate)
                elif(counter<100):
                    temp=random.randrange(1,2)/100
                    rate+=temp
                elif(counter<130):
                    temp=0
                    rate+=(random.randrange(1,5)/10)
                    print("rate :",rate)
                elif(counter<200):
                    print("constantrate:",rate)
                    temp=(random.randrange(1,5)/10)
                    rate+=temp
                elif(counter<300):
                    temp=0
                    rate-=(random.randrange(1,5)/100)
                elif(counter<320):
                    temp=random.randrange(1,5)
                    rate+=temp
                elif(counter<340):
                    temp=0
                    rate+=(random.randrange(3,9)/10)
                elif(counter<400):
                    temp=random.randrange(3,9)/10
                    rate+=temp
                elif(counter>400):
                    temp=0
                    rate-=(random.randrange(3,9)/100)
                counter+=1
                print("rate:",rate)
                print("interval : ",interval)
                print("counter:",counter)
                time.sleep(interval)


        
if __name__ == "__main__":
    input=sys.argv[1]
    send_at(inputrate,input)
