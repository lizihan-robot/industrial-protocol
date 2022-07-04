	
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import serial


class ModbusRtuMaster:
    def __init__(self, port, baudrate):
        self.port = port
        self.baudrate = baudrate
        self.modbus_rtu_master = None

    def run(self, timeout=10):
        self.modbus_rtu_master = modbus_rtu.RtuMaster(serial.Serial(port=self.port, 
                                baudrate=self.baudrate, 
                                bytesize=8, 
                                parity='E', 
                                stopbits=1))
        self.modbus_rtu_master.set_timeout(timeout)
        print("[INFO]: Modbus_Rtu Master running!!")

    def stop(self):
        if self.modbus_rtu_master:
            self.modbus_rtu_master.close()


    def write_single(self, address, value):
        res = None
        try:
            res = self.modbus_rtu_master.execute(1, cst.WRITE_SINGLE_COIL, address, output_value=value)
            print("[Mobuds INFO]: write command successful !\n[res]: {}".format(res))
        except Exception as e:
            traceback.print_exc()
            print("[Mobuds ERROR]: write command error \n[ERROR INFO]: {}\n[res]: {}".format(e, res))

    def read_single(self, address):
        res = None
        try:
            return self.modbus_rtu_master.execute(1, cst.READ_COILS, address, 1)
        except Exception as e:
            traceback.print_exc()
            print("[Mobuds ERROR]: read command error \n[ERROR INFO]: {}\n[res]: {}".format(e, res))

    def wait_until(self, *args):
        """
        """
        print("modbus client is waiting for io:",args)
        while True:
            for arg in args:
                cmd = self.read_single(arg)[0]
                if cmd:
                    print("[Modbus] get a command signal:{},value:{}".format(arg, cmd))
                    return cmd
                else:
                    time.sleep(0.2)
