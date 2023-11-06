import codecs
# -*- coding: UTF-8 -*-

import paho.mqtt.client as mqtt
import json
import time
import schedule  
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_tcp
import struct
import threading
import struct
import IAQ_Sensor
import PowerMeter


def Publish_IAQ():
    try:
        IAQ_Data = IAQ_Sensor.GetIAQ('/dev/ttyS1',1)
        client = mqtt.Client()
        client.on_connect
        client.username_pw_set('TlCpEHdUJUTgiLNovuYO','XXX')
        client.connect('thingsboard.cloud', 1883, 60)
        TimeStamp = IAQ_Sensor.Current_ms()
        
        if (IAQ_Data[0] != 0):
            payload_iaq = {"ts": TimeStamp,
                           "values":{"CO2":IAQ_Data[0],"PM2.5":IAQ_Data[1],"RH":IAQ_Data[2]/100,"Temp":IAQ_Data[3]/100}
                           }
            print(client.publish("v1/devices/me/telemetry", json.dumps(payload_iaq)))
            time.sleep(5)
    except:
        pass

def Publish_MaMainPower():
    try:
        PowerFrq_Value = PowerMeter.Read_MaPowerFreq()
        PowerVoltage = PowerMeter.Read_MaPowerVoltage()
        PowerCurrnet = PowerMeter.Read_MaPowerCurrnet()
        PowerkW = PowerMeter.Read_MaPowerkW()
        PowerkVAR = PowerMeter.Read_MaPowerkVAR()
        PowerkVAS = PowerMeter.Read_MaPowerkVAS()
        MainPowerPF = PowerMeter.Read_MaPowerPF()
        MainPowerDM = PowerMeter.Read_MaPowerDM()
        MainPowerAE = PowerMeter.Read_MaPowerAE()

        #MQTT_Connect()
        client = mqtt.Client()
        client.on_connect
        client.username_pw_set('i4qe8Jbakwyt0rSeE4Ko','')
        print(client.connect('thingsboard.cloud', 1883, 60))
        TimeStamp = IAQ_Sensor.Current_ms()
        payload_MaPower = {"ts": TimeStamp,
                            "values":{"MaPowerFrq":PowerFrq_Value,
                                      "MaPowerV1":PowerVoltage[0],
                                      "MaPowerV2":PowerVoltage[1],
                                      "MaPowerV3":PowerVoltage[2],
                                      "MaPowerVavg":PowerVoltage[3],
                                      "MaPowerI1":PowerCurrnet[0],
                                      "MaPowerI2":PowerCurrnet[1],
                                      "MaPowerI3":PowerCurrnet[2],
                                      "MaPowerIavg":PowerCurrnet[3],
                                      "MaPowerkWavg":PowerkW[3],
                                      "MaPowerkVARavg":PowerkVAR[3],
                                      "MaPowerkVASavg":PowerkVAS[3],
                                      "MaPowerPF":MainPowerPF[3],
                                      "MaPowerDM":MainPowerDM,
                                      "MaPowerAE":MainPowerAE
                                      }
                       }
        
        print(client.publish("v1/devices/me/telemetry", json.dumps(payload_MaPower)))
        time.sleep(5)
    except:
        PowerMeter.modbus_connection()
        print ("error")

def Publish_SubMaPower():
    try:
        for i in range(4):
            PowerCurrnet = PowerMeter.Read_SubAPowerCurrnet(i)
            PowerkW = PowerMeter.Read_SubAPowerkW(i)
            PowerkVAR = PowerMeter.Read_SubAPowerkVAR(i)
            PowerPF = PowerMeter.Read_SubAPowerPF(i)
            PowerAE = PowerMeter.Read_SubAPowerAE(i)            
            
            
            #MQTT_Connect()
            clientsub = mqtt.Client()
            clientsub.on_connect
            clientsub.username_pw_set('i4qe8Jbakwyt0rSeE4Ko','')
            print(clientsub.connect('thingsboard.cloud', 1883, 60))
            TimeStamp = IAQ_Sensor.Current_ms()
            payload_iaq = {"ts": TimeStamp,
                            "values":{"SubPowerI1"+str(i):PowerCurrnet[0],
                                      "SubPowerI2"+str(i):PowerCurrnet[1],
                                      "SubPowerI3"+str(i):PowerCurrnet[2],
                                      "SubPowerIavg"+str(i):PowerCurrnet[3],
                                      "SubPowerkW1"+str(i):PowerkW[0],
                                      "SubPowerkW2"+str(i):PowerkW[1],
                                      "SubPowerkW3"+str(i):PowerkW[2],
                                      "SubPowerkWT"+str(i):PowerkW[3],
                                      "SubPowerkVAR1"+str(i):PowerkVAR[0],
                                      "SubPowerkVAR2"+str(i):PowerkVAR[1],
                                      "SubPowerkVAR3"+str(i):PowerkVAR[2],
                                      "SubPowerkVAT"+str(i):PowerkVAR[3],
                                      "SubPowerPF1"+str(i):PowerPF[0],
                                      "SubPowerPF2"+str(i):PowerPF[1],
                                      "SubPowerPF3"+str(i):PowerPF[2],
                                      "SubPowerPFT"+str(i):PowerPF[3],
                                      "SubPowerAE1"+str(i):PowerAE[0],
                                      "SubPowerAE2"+str(i):PowerAE[1],
                                      "SubPowerAE3"+str(i):PowerAE[2],
                                      "SubPowerAET"+str(i):PowerAE[3],
                                      }
                       }
            print(json.dumps(payload_iaq))
            print(clientsub.publish("v1/devices/me/telemetry", json.dumps(payload_iaq)))
            time.sleep(5)
    except:
        PowerMeter.modbus_connection()
        print ("error")

def Publish_MbMainPower():
    try:
        PowerFrq_Value = PowerMeter.Read_MbPowerFreq()
        PowerVoltage = PowerMeter.Read_MbPowerVoltage()
        PowerCurrnet = PowerMeter.Read_MbPowerCurrnet()
        PowerkW = PowerMeter.Read_MbPowerkW()
        PowerkVAR = PowerMeter.Read_MbPowerkVAR()
        PowerkVAS = PowerMeter.Read_MbPowerkVAS()
        MainPowerPF = PowerMeter.Read_MbPowerPF()
        MainPowerDM = PowerMeter.Read_MbPowerDM()
        MainPowerAE = PowerMeter.Read_MbPowerAE()

        #MQTT_Connect()
        client = mqtt.Client()
        client.on_connect
        client.username_pw_set('i4qe8Jbakwyt0rSeE4Ko','')
        print(client.connect('thingsboard.cloud', 1883, 60))
        TimeStamp = IAQ_Sensor.Current_ms()
        payload_MaPower = {"ts": TimeStamp,
                            "values":{"MbPowerFrq":PowerFrq_Value,
                                      "MbPowerV1":PowerVoltage[0],
                                      "MbPowerV2":PowerVoltage[1],
                                      "MbPowerV3":PowerVoltage[2],
                                      "MbPowerVavg":PowerVoltage[3],
                                      "MbPowerI1":PowerCurrnet[0],
                                      "MbPowerI2":PowerCurrnet[1],
                                      "MbPowerI3":PowerCurrnet[2],
                                      "MbPowerIavg":PowerCurrnet[3],
                                      "MbPowerkWavg":PowerkW[3],
                                      "MbPowerkVARavg":PowerkVAR[3],
                                      "MbPowerkVASavg":PowerkVAS[3],
                                      "MbPowerPF":MainPowerPF[3],
                                      "MbPowerDM":MainPowerDM,
                                      "MbPowerAE":MainPowerAE
                                      }
                       }
        
        print(client.publish("v1/devices/me/telemetry", json.dumps(payload_MaPower)))
        time.sleep(5)
    except:
        PowerMeter.modbus_connection()
        print ("error")

def Publish_SubMbPower():
    try:
        for i in range(4):
            PowerCurrnet = PowerMeter.Read_SubBPowerCurrnet(i)
            PowerkW = PowerMeter.Read_SubBPowerkW(i)
            PowerkVAR = PowerMeter.Read_SubBPowerkVAR(i)
            PowerPF = PowerMeter.Read_SubBPowerPF(i)
            PowerAE = PowerMeter.Read_SubBPowerAE(i)            
            
            
            #MQTT_Connect()
            clientsub = mqtt.Client()
            clientsub.on_connect
            clientsub.username_pw_set('i4qe8Jbakwyt0rSeE4Ko','')
            print(clientsub.connect('thingsboard.cloud', 1883, 60))
            TimeStamp = IAQ_Sensor.Current_ms()
            payload_iaq = {"ts": TimeStamp,
                            "values":{"SubPowerI1"+str(i+4):PowerCurrnet[0],
                                      "SubPowerI2"+str(i+4):PowerCurrnet[1],
                                      "SubPowerI3"+str(i+4):PowerCurrnet[2],
                                      "SubPowerIavg"+str(i+4):PowerCurrnet[3],
                                      "SubPowerkW1"+str(i+4):PowerkW[0],
                                      "SubPowerkW2"+str(i+4):PowerkW[1],
                                      "SubPowerkW3"+str(i+4):PowerkW[2],
                                      "SubPowerkWT"+str(i+4):PowerkW[3],
                                      "SubPowerkVAR1"+str(i+4):PowerkVAR[0],
                                      "SubPowerkVAR2"+str(i+4):PowerkVAR[1],
                                      "SubPowerkVAR3"+str(i+4):PowerkVAR[2],
                                      "SubPowerkVAT"+str(i+4):PowerkVAR[3],
                                      "SubPowerPF1"+str(i+4):PowerPF[0],
                                      "SubPowerPF2"+str(i+4):PowerPF[1],
                                      "SubPowerPF3"+str(i+4):PowerPF[2],
                                      "SubPowerPFT"+str(i+4):PowerPF[3],
                                      "SubPowerAE1"+str(i+4):PowerAE[0],
                                      "SubPowerAE2"+str(i+4):PowerAE[1],
                                      "SubPowerAE3"+str(i+4):PowerAE[2],
                                      "SubPowerAET"+str(i+4):PowerAE[3],
                                      }
                       }
            print(json.dumps(payload_iaq))
            print(clientsub.publish("v1/devices/me/telemetry", json.dumps(payload_iaq)))
            time.sleep(5)
    except:
        PowerMeter.modbus_connection()
        print ("error")

Publish_IAQ()
Publish_MaMainPower()
Publish_SubMaPower()
Publish_MbMainPower()
Publish_SubMbPower()

schedule.every(5).minutes.do(Publish_IAQ)
schedule.every(5).minutes.do(Publish_MaMainPower)
schedule.every(5).minutes.do(Publish_SubMaPower)
schedule.every(5).minutes.do(Publish_MbMainPower)
schedule.every(5).minutes.do(Publish_SubMbPower)



if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)