import codecs
import time
import serial
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import schedule 
import json
import ssl
import paho.mqtt.client as mqtt


def Current_ms():
    time_stamp_s = int(time.time()) # 轉成時間戳
    d = time_stamp_s % 60
    time_stamp_set = time_stamp_s - d
    time_stamp_ms = time_stamp_set * 1000
    
    return time_stamp_ms

def Publish_IAQ():
    try:
        client = mqtt.Client()
        client.on_connect
        client.username_pw_set('fkvRuzah5rKvCb9mZbjG','XXX')
        client.connect('thingsboard.cloud', 1883, 60)
        TimeStamp = Current_ms()
        IAQ_Data = GetIAQ('/dev/ttyS1',1)
        if (IAQ_Data[0] != 0):
            payload_iaq = {"ts": TimeStamp,
                           "values":{"CO2":IAQ_Data[0],"PM2.5":IAQ_Data[1],"RH":IAQ_Data[2]/100,"Temp":IAQ_Data[3]/100}
                           }
            print(client.publish("v1/devices/me/telemetry", json.dumps(payload_iaq)))
            time.sleep(5)
    except:
        pass


def GetIAQ(PORT,ID):
    try:
        master = modbus_rtu.RtuMaster(serial.Serial(port=PORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0))
        master.set_timeout(5.0)
        master.set_verbose(True)
        IAQ_Data = master.execute(ID, cst.READ_INPUT_REGISTERS, 5, 4)
        time.sleep(1)
        return (IAQ_Data)
        
    except:
        IAQ_Data = [0,0,0,0]
        return (IAQ_Data)

schedule.every(5).minutes.do(Publish_IAQ)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
    