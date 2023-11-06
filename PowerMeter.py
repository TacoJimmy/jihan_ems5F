import codecs
# -*- coding: UTF-8 -*-

import paho.mqtt.client as mqtt
import json
import time
import schedule  
import serial
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import threading
import struct

try:
    master = modbus_rtu.RtuMaster(serial.Serial(port='/dev/ttyS4', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
    master.set_timeout(5.0)
    master.set_verbose(True)
except:
    pass

def modbus_connection():
    global master
    try:
        master = modbus_rtu.RtuMaster(serial.Serial(port='/dev/ttyS4', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
        master.set_timeout(5.0)
        master.set_verbose(True)
    except:
        pass

#if datat -999999~999999
# num1 is hi bit, num2 is lo bit
def conv(num1,num2):
    #check negative
    num1_negative = (num1>>15) & 0x1
    num2_negative = (num2>>15) & 0x1
    
    if num1_negative == 1:
        num1_conv = (0xFFFF - num1)
        num1_conv = num1_conv * (-1)
    else:
        num1_conv = num1
    if num2_negative == 1:
        num2_conv = (0xFFFF - num2)
        num2_conv = num2_conv * (-1)
    else:
        num2_conv = num2
    num = (num2_conv*32768)+num1_conv
    
    return num

def VoltageConv(num1, num2):
    combined_num = (num1 << 16) | num2
    float_num = round(combined_num/10,1)
    return float_num

def SignConv(num1):
    Sign_num = (num1 >> 15) 
    if Sign_num == 1 :
        Sign_num = num1*(-1)
    else:
        Sign_num = num1
    return Sign_num

def CurrntConv(num1, num2):
    combined_num = (num1 << 16) | num2
    float_num = round(combined_num/1000,1)
    return float_num

def kWConv(num1, num2):
    combined_num = (num1 << 16) | num2
    packed_num = struct.pack('i', combined_num)
    
    return packed_num

########################
#MaPower
########################

def Read_MaPowerFreq():
    PowerFreq_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4096, 1)
    #time.sleep(1)
    PowerFreq_Value = round(PowerFreq_Data[0]*0.01,2)
    return PowerFreq_Value

def Read_MaPowerVoltage():
    PowerVoltage_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4097, 8)
    #time.sleep(1)
    PowerVoltage_V1 = VoltageConv(PowerVoltage_Data[0], PowerVoltage_Data[1])
    PowerVoltage_V2 = VoltageConv(PowerVoltage_Data[2], PowerVoltage_Data[3])
    PowerVoltage_V3 = VoltageConv(PowerVoltage_Data[4], PowerVoltage_Data[5])
    PowerVoltage_Vavg = VoltageConv(PowerVoltage_Data[6], PowerVoltage_Data[7])
    return PowerVoltage_V1,PowerVoltage_V2,PowerVoltage_V3,PowerVoltage_Vavg

def Read_MaPowerCurrnet():
    PowerCurrnet_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4113, 8)
    #time.sleep(1)
    PowerCurrnet_I1 = CurrntConv(PowerCurrnet_Data[0], PowerCurrnet_Data[1])
    PowerCurrnet_I2 = CurrntConv(PowerCurrnet_Data[2], PowerCurrnet_Data[3])
    PowerCurrnet_I3 = CurrntConv(PowerCurrnet_Data[4], PowerCurrnet_Data[5])
    PowerCurrnet_Iavg = CurrntConv(PowerCurrnet_Data[6], PowerCurrnet_Data[7])
    return PowerCurrnet_I1,PowerCurrnet_I2,PowerCurrnet_I3,PowerCurrnet_Iavg

def Read_MaPowerkW():
    PowerkW_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4123, 8)
    #time.sleep(1)
    PowerkW_I1 = conv(PowerkW_Data[1], PowerkW_Data[0])
    PowerkW_I2 = conv(PowerkW_Data[3], PowerkW_Data[2])
    PowerkW_I3 = conv(PowerkW_Data[5], PowerkW_Data[4])
    PowerkW_Iavg = conv(PowerkW_Data[7], PowerkW_Data[6])
    return PowerkW_I1, PowerkW_I2, PowerkW_I3, PowerkW_Iavg

def Read_MaPowerkVAR():
    PowerkVAR_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4131, 8)
    #time.sleep(1)
    PowerkVAR_I1 = conv(PowerkVAR_Data[1], PowerkVAR_Data[0])
    PowerkVAR_I2 = conv(PowerkVAR_Data[3], PowerkVAR_Data[2])
    PowerkVAR_I3 = conv(PowerkVAR_Data[5], PowerkVAR_Data[4])
    PowerkVAR_Iavg = conv(PowerkVAR_Data[7], PowerkVAR_Data[6])
    return PowerkVAR_I1, PowerkVAR_I2, PowerkVAR_I3, PowerkVAR_Iavg

def Read_MaPowerkVAS():
    PowerkVAS_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4139, 8)
    #time.sleep(1)
    PowerkVAS_I1 = conv(PowerkVAS_Data[1], PowerkVAS_Data[0])
    PowerkVAS_I2 = conv(PowerkVAS_Data[3], PowerkVAS_Data[2])
    PowerkVAS_I3 = conv(PowerkVAS_Data[5], PowerkVAS_Data[4])
    PowerkVAS_Iavg = conv(PowerkVAS_Data[7], PowerkVAS_Data[6])
    return PowerkVAS_I1, PowerkVAS_I2, PowerkVAS_I3, PowerkVAS_Iavg

def Read_MaPowerPF():
    PowerPF_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4147, 4)
    #time.sleep(1)
    PowerPF1 = SignConv(PowerPF_Data[0])/1000
    PowerPF2 = SignConv(PowerPF_Data[1])/1000
    PowerPF3 = SignConv(PowerPF_Data[2])/1000
    PowerPFT = SignConv(PowerPF_Data[3])/1000
    return PowerPF1, PowerPF2, PowerPF3, PowerPFT

def Read_MaPowerDM():
    PowerDM_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4161, 2)
    PowerkW_DM = conv(PowerDM_Data[1], PowerDM_Data[0])
    
    return PowerkW_DM

def Read_MaPowerAE():
    PowerAE_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 4167, 2)
    PowerkWh_AE = conv(PowerAE_Data[1], PowerAE_Data[0])
    
    return PowerkWh_AE

########################
#MbPower
########################

def Read_MbPowerFreq():
    PowerFreq_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8192, 1)
    #time.sleep(1)
    PowerFreq_Value = round(PowerFreq_Data[0]*0.01,2)
    return PowerFreq_Value

def Read_MbPowerVoltage():
    PowerVoltage_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8193, 8)
    #time.sleep(1)
    PowerVoltage_V1 = VoltageConv(PowerVoltage_Data[0], PowerVoltage_Data[1])
    PowerVoltage_V2 = VoltageConv(PowerVoltage_Data[2], PowerVoltage_Data[3])
    PowerVoltage_V3 = VoltageConv(PowerVoltage_Data[4], PowerVoltage_Data[5])
    PowerVoltage_Vavg = VoltageConv(PowerVoltage_Data[6], PowerVoltage_Data[7])
    return PowerVoltage_V1,PowerVoltage_V2,PowerVoltage_V3,PowerVoltage_Vavg

def Read_MbPowerCurrnet():
    PowerCurrnet_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8209, 8)
    #time.sleep(1)
    PowerCurrnet_I1 = CurrntConv(PowerCurrnet_Data[0], PowerCurrnet_Data[1])
    PowerCurrnet_I2 = CurrntConv(PowerCurrnet_Data[2], PowerCurrnet_Data[3])
    PowerCurrnet_I3 = CurrntConv(PowerCurrnet_Data[4], PowerCurrnet_Data[5])
    PowerCurrnet_Iavg = CurrntConv(PowerCurrnet_Data[6], PowerCurrnet_Data[7])
    return PowerCurrnet_I1,PowerCurrnet_I2,PowerCurrnet_I3,PowerCurrnet_Iavg

def Read_MbPowerkW():
    PowerkW_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8219, 8)
    #time.sleep(1)
    PowerkW_I1 = conv(PowerkW_Data[1], PowerkW_Data[0])
    PowerkW_I2 = conv(PowerkW_Data[3], PowerkW_Data[2])
    PowerkW_I3 = conv(PowerkW_Data[5], PowerkW_Data[4])
    PowerkW_Iavg = conv(PowerkW_Data[7], PowerkW_Data[6])
    return PowerkW_I1, PowerkW_I2, PowerkW_I3, PowerkW_Iavg

def Read_MbPowerkVAR():
    PowerkVAR_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8227, 8)
    #time.sleep(1)
    PowerkVAR_I1 = conv(PowerkVAR_Data[1], PowerkVAR_Data[0])
    PowerkVAR_I2 = conv(PowerkVAR_Data[3], PowerkVAR_Data[2])
    PowerkVAR_I3 = conv(PowerkVAR_Data[5], PowerkVAR_Data[4])
    PowerkVAR_Iavg = conv(PowerkVAR_Data[7], PowerkVAR_Data[6])
    return PowerkVAR_I1, PowerkVAR_I2, PowerkVAR_I3, PowerkVAR_Iavg

def Read_MbPowerkVAS():
    PowerkVAS_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8235, 8)
    #time.sleep(1)
    PowerkVAS_I1 = conv(PowerkVAS_Data[1], PowerkVAS_Data[0])
    PowerkVAS_I2 = conv(PowerkVAS_Data[3], PowerkVAS_Data[2])
    PowerkVAS_I3 = conv(PowerkVAS_Data[5], PowerkVAS_Data[4])
    PowerkVAS_Iavg = conv(PowerkVAS_Data[7], PowerkVAS_Data[6])
    return PowerkVAS_I1, PowerkVAS_I2, PowerkVAS_I3, PowerkVAS_Iavg

def Read_MbPowerPF():
    PowerPF_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8243, 4)
    #time.sleep(1)
    PowerPF1 = SignConv(PowerPF_Data[0])/1000
    PowerPF2 = SignConv(PowerPF_Data[1])/1000
    PowerPF3 = SignConv(PowerPF_Data[2])/1000
    PowerPFT = SignConv(PowerPF_Data[3])/1000
    return PowerPF1, PowerPF2, PowerPF3, PowerPFT

def Read_MbPowerDM():
    PowerDM_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8257, 2)
    PowerkW_DM = conv(PowerDM_Data[1], PowerDM_Data[0])
    
    return PowerkW_DM

def Read_MbPowerAE():
    PowerAE_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, 8263, 2)
    PowerkWh_AE = conv(PowerAE_Data[1], PowerAE_Data[0])
    
    return PowerkWh_AE

########################
#SubAPower
########################

def Read_SubAPowerCurrnet(Cound):
    Reg_addr = 5120+768*Cound
    PowerCurrnet_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerCurrnet_I1 = CurrntConv(PowerCurrnet_Data[0], PowerCurrnet_Data[1])
    PowerCurrnet_I2 = CurrntConv(PowerCurrnet_Data[2], PowerCurrnet_Data[3])
    PowerCurrnet_I3 = CurrntConv(PowerCurrnet_Data[4], PowerCurrnet_Data[5])
    PowerCurrnet_Iavg = CurrntConv(PowerCurrnet_Data[6], PowerCurrnet_Data[7])
    return PowerCurrnet_I1,PowerCurrnet_I2,PowerCurrnet_I3,PowerCurrnet_Iavg

def Read_SubAPowerkW(Cound):
    Reg_addr = 5128 + 768 * Cound
    PowerkW_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerkW_I1 = conv(PowerkW_Data[1], PowerkW_Data[0])
    PowerkW_I2 = conv(PowerkW_Data[3], PowerkW_Data[2])
    PowerkW_I3 = conv(PowerkW_Data[5], PowerkW_Data[4])
    PowerkW_Iavg = conv(PowerkW_Data[7], PowerkW_Data[6])
    return PowerkW_I1, PowerkW_I2, PowerkW_I3, PowerkW_Iavg

def Read_SubAPowerkVAR(Cound):
    Reg_addr = 5136 + 768 * Cound
    PowerkVAR_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerkVAR_I1 = conv(PowerkVAR_Data[1], PowerkVAR_Data[0])
    PowerkVAR_I2 = conv(PowerkVAR_Data[3], PowerkVAR_Data[2])
    PowerkVAR_I3 = conv(PowerkVAR_Data[5], PowerkVAR_Data[4])
    PowerkVAR_Iavg = conv(PowerkVAR_Data[7], PowerkVAR_Data[6])
    return PowerkVAR_I1, PowerkVAR_I2, PowerkVAR_I3, PowerkVAR_Iavg

def Read_SubAPowerkVAS(Cound):
    Reg_addr = 5144 + 768 * Cound
    PowerkVAS_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    time.sleep(1)
    PowerkVAS_I1 = conv(PowerkVAS_Data[1], PowerkVAS_Data[0])
    PowerkVAS_I2 = conv(PowerkVAS_Data[3], PowerkVAS_Data[2])
    PowerkVAS_I3 = conv(PowerkVAS_Data[5], PowerkVAS_Data[4])
    PowerkVAS_Iavg = conv(PowerkVAS_Data[7], PowerkVAS_Data[6])
    return PowerkVAS_I1, PowerkVAS_I2, PowerkVAS_I3, PowerkVAS_Iavg

def Read_SubAPowerPF(Cound):
    Reg_addr = 5152 + 768 * Cound
    PowerPF_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 4)
    PowerPF1 = SignConv(PowerPF_Data[0])/1000
    PowerPF2 = SignConv(PowerPF_Data[1])/1000
    PowerPF3 = SignConv(PowerPF_Data[2])/1000
    PowerPFT = SignConv(PowerPF_Data[3])/1000
    return PowerPF1, PowerPF2, PowerPF3, PowerPFT

def Read_SubAPowerAE(Cound):
    Reg_addr = 5188 + 768 * Cound
    PowerAE_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    PowerAE1 = conv(PowerAE_Data[1], PowerAE_Data[0])
    PowerAE2 = conv(PowerAE_Data[3], PowerAE_Data[2])
    PowerAE3 = conv(PowerAE_Data[5], PowerAE_Data[4])
    PowerAET = conv(PowerAE_Data[7], PowerAE_Data[6])
    return PowerAE1, PowerAE2, PowerAE3, PowerAET

########################
#SubBPower
########################

def Read_SubBPowerCurrnet(Cound):
    Reg_addr = 5120+768*Cound
    PowerCurrnet_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerCurrnet_I1 = CurrntConv(PowerCurrnet_Data[0], PowerCurrnet_Data[1])
    PowerCurrnet_I2 = CurrntConv(PowerCurrnet_Data[2], PowerCurrnet_Data[3])
    PowerCurrnet_I3 = CurrntConv(PowerCurrnet_Data[4], PowerCurrnet_Data[5])
    PowerCurrnet_Iavg = CurrntConv(PowerCurrnet_Data[6], PowerCurrnet_Data[7])
    return PowerCurrnet_I1,PowerCurrnet_I2,PowerCurrnet_I3,PowerCurrnet_Iavg

def Read_SubBPowerkW(Cound):
    Reg_addr = 5128 + 768 * Cound
    PowerkW_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerkW_I1 = conv(PowerkW_Data[1], PowerkW_Data[0])
    PowerkW_I2 = conv(PowerkW_Data[3], PowerkW_Data[2])
    PowerkW_I3 = conv(PowerkW_Data[5], PowerkW_Data[4])
    PowerkW_Iavg = conv(PowerkW_Data[7], PowerkW_Data[6])
    return PowerkW_I1, PowerkW_I2, PowerkW_I3, PowerkW_Iavg

def Read_SubBPowerkVAR(Cound):
    Reg_addr = 5136 + 768 * Cound
    PowerkVAR_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    #time.sleep(1)
    PowerkVAR_I1 = conv(PowerkVAR_Data[1], PowerkVAR_Data[0])
    PowerkVAR_I2 = conv(PowerkVAR_Data[3], PowerkVAR_Data[2])
    PowerkVAR_I3 = conv(PowerkVAR_Data[5], PowerkVAR_Data[4])
    PowerkVAR_Iavg = conv(PowerkVAR_Data[7], PowerkVAR_Data[6])
    return PowerkVAR_I1, PowerkVAR_I2, PowerkVAR_I3, PowerkVAR_Iavg

def Read_SubBPowerkVAS(Cound):
    Reg_addr = 5144 + 768 * Cound
    PowerkVAS_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    time.sleep(1)
    PowerkVAS_I1 = conv(PowerkVAS_Data[1], PowerkVAS_Data[0])
    PowerkVAS_I2 = conv(PowerkVAS_Data[3], PowerkVAS_Data[2])
    PowerkVAS_I3 = conv(PowerkVAS_Data[5], PowerkVAS_Data[4])
    PowerkVAS_Iavg = conv(PowerkVAS_Data[7], PowerkVAS_Data[6])
    return PowerkVAS_I1, PowerkVAS_I2, PowerkVAS_I3, PowerkVAS_Iavg

def Read_SubBPowerPF(Cound):
    Reg_addr = 5152 + 768 * Cound
    PowerPF_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 4)
    PowerPF1 = SignConv(PowerPF_Data[0])/1000
    PowerPF2 = SignConv(PowerPF_Data[1])/1000
    PowerPF3 = SignConv(PowerPF_Data[2])/1000
    PowerPFT = SignConv(PowerPF_Data[3])/1000
    return PowerPF1, PowerPF2, PowerPF3, PowerPFT

def Read_SubBPowerAE(Cound):
    Reg_addr = 5188 + 768 * Cound
    PowerAE_Data = master.execute(1, cst.READ_HOLDING_REGISTERS, Reg_addr, 8)
    PowerAE1 = conv(PowerAE_Data[1], PowerAE_Data[0])
    PowerAE2 = conv(PowerAE_Data[3], PowerAE_Data[2])
    PowerAE3 = conv(PowerAE_Data[5], PowerAE_Data[4])
    PowerAET = conv(PowerAE_Data[7], PowerAE_Data[6])
    return PowerAE1, PowerAE2, PowerAE3, PowerAET


def Send_PowerMeter():
    print (Read_MaPowerFreq())
    print (Read_MaPowerVoltage())

if __name__ == '__main__':
    while True:
        
        i =2
        print (str(i+4))
        
        '''
        try:
            for i in range(4):
                print (Read_SubPowerCurrnet(i))
                print (Read_SubPowerkW(i))
                print (Read_SubPowerkVAR(i))
                print (Read_SubPowerPF(i))
                print (Read_SubPowerAE(i))
        except:
            modbus_connection()
        '''
        time.sleep(5)