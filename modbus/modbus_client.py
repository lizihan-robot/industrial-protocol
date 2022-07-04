"""
Copyright (c) XYZ Robotics Inc. - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Author: MinWu <min.wu@xyzrobotics.ai>, 2022/04/01
"""


'''
a ModbusTcpClient implementation using modbus_tk
ModbusTcpClient is mainly for communication with PLC
'''

from collections import namedtuple
from collections.abc import Iterable
import struct, sys, time
import modbus_tk.defines as cst
from multiprocessing import Lock
from modbus_tk import modbus_tcp

from .glog import logger as logging

#supported format
DisplayFormat = namedtuple('DisplayFormat', ['format', 'bytes', 'info'])

FMT_SIGNED_WORD = DisplayFormat('h', 2, "signed word")
FMT_UNSIGNED_WORD = DisplayFormat('H', 2, "unsigned word")
FMT_SIGNED_2WORD = DisplayFormat('i', 4, "signed dword")
FMT_UNSIGNED_2WORD = DisplayFormat('I', 4, "unsigned dword")
FMT_FLOAT_2WORD = DisplayFormat('f', 4, "float")
FMT_SIGNED_4WORD = DisplayFormat('q', 8, "signed 64 bit")
FMT_UNSIGNED_4WORD = DisplayFormat('Q', 8, "unsigned 64 bit")
FMT_DOUBLE_4WORD = DisplayFormat('d', 8, "double")

BYTE_ORDER_BIG_ENDIAN = 0
BYTE_ORDER_LITTLE_ENDIAN = 1
BYTE_ORDER_BIG_ENDIAN_SWAP = 2
BYTE_ORDER_LITTLE_ENDIAN_SWAP = 3


def _swap_little_endian_2bytes(src_bytes: bytes) -> bytes:
    """
    byte order : BA
    """
    rb = b''
    assert(len(src_bytes)%2==0)
    for i in range(len(src_bytes)):
        if i%2 == 0:
            rb += bytes([src_bytes[i+1], src_bytes[i]])
    return rb

def _swap_little_endian_4bytes(src_bytes: bytes) -> bytes:
    """
    byte order : DC BA
    """
    rb = b''
    assert(len(src_bytes)%4==0)
    for i in range(len(src_bytes)):
        if i%4 == 0:
            rb += bytes([src_bytes[i+3], src_bytes[i+2], src_bytes[i+1], src_bytes[i]])
    return rb

def _swap_byte_big_endian_4bytes(src_bytes: bytes) -> bytes:
    """
    byte order : BA DC
    """
    rb = b''
    assert(len(src_bytes)%4==0)
    for i in range(len(src_bytes)):
        if i%4 == 0:
            rb += bytes([src_bytes[i+1], src_bytes[i], src_bytes[i+3], src_bytes[i+2]])
    return rb

def _swap_byte_little_endian_4bytes(src_bytes: bytes) -> bytes:
    """
    byte order : CD AB
    """
    rb = b''
    assert(len(src_bytes)%4==0)
    for i in range(len(src_bytes)):
        if i%4 == 0:
            rb += bytes([src_bytes[i+2], src_bytes[i+3], src_bytes[i], src_bytes[i+1]])
    return rb

def _swap_little_endian_8bytes(src_bytes: bytes) -> bytes:
    """
    byte order : HG FE DC BA
    """
    rb = b''
    assert(len(src_bytes)%8==0)
    for i in range(len(src_bytes)):
        if i%8 == 0:
            rb += bytes([src_bytes[i+7], src_bytes[i+6], src_bytes[i+5], src_bytes[i+4],
                            src_bytes[i+3], src_bytes[i+2], src_bytes[i+1], src_bytes[i]])
    return rb

def _swap_byte_big_endian_8bytes(src_bytes: bytes) -> bytes:
    """
    byte order : BA DC FE HG
    """
    rb = b''
    assert(len(src_bytes)%8==0)
    for i in range(len(src_bytes)):
        if i%8 == 0:
            rb += bytes([src_bytes[i+1], src_bytes[i], src_bytes[i+3], src_bytes[i+2],
                            src_bytes[i+5], src_bytes[i+4], src_bytes[i+7], src_bytes[i+6]])
    return rb

def _swap_byte_little_endian_8bytes(src_bytes: bytes) -> bytes:
    """
    byte order : GH EF CD AB
    """
    rb = b''
    assert(len(src_bytes)%8==0)
    for i in range(len(src_bytes)):
        if i%8 == 0:
            rb += bytes([src_bytes[i+6], src_bytes[i+7], src_bytes[i+4], src_bytes[i+5],
                            src_bytes[i+2], src_bytes[i+3], src_bytes[i], src_bytes[i+1]])
    return rb


class ModbusTcpClient(object):
    """ModbusTcpClient act as socket client, aka ModbusTcp Master"""

    def __init__(self, server_ip="127.0.0.1", port=502, signed=True, time_out=5.0, max_retry=20, slave_id=1):
        bybe_order = sys.byteorder.capitalize()
        assert(bybe_order == "Little")
        self.modbus_server_ip = server_ip
        self.port = port
        self.signed = signed
        self.time_out = time_out
        self.max_retry = max_retry
        self.slave_id = slave_id
        self._client_lock = Lock() # redundant as TcpMaster.execute() is thread safe

    def connect(self):
        connected  = self._connect_modbus_server(server_ip=self.modbus_server_ip, port=self.port, \
                                                 time_out=self.time_out, max_retry=self.max_retry)
        return connected

    def set_holding_register_signed(self, signed):
        """
        defines holding register to be signed or unsigned value
        """
        self.signed = signed
    
    def set_slave_id(self, slave_id):
        self.slave_id = slave_id

    def _connect_modbus_server(self, server_ip, port, time_out, max_retry):
        for _ in range(max_retry):
            try:
                # init yet not connect to socket server
                # open and close connection each time in TcpMaster.execute()
                self._modbus_client = modbus_tcp.TcpMaster(host=server_ip, port=port) 
                self._modbus_client.set_timeout(time_out)
                # connect
                self._modbus_client.open()
                # heart beat holding register
                self.read_holding_registers(0, 1)
                logging.info("[Modbus] connect success to slave {}".format(server_ip))
                return True
                break
            except Exception as e:
                self._modbus_client = None
                logging.error("[Modbus-Error] can not connect to slave! the error is: {}".format(e))
                time.sleep(time_out)
                continue
        return False
   
    def close(self):
        if self._modbus_client:
            self._modbus_client.close()     

    @staticmethod
    def pack_values(src_values, signed, display_format, endianness):
        """
        pack actual values to a list/tuple of holding registers
        the results can be used in TcpMaster.execute() "write holding regesiters" directly 
        
        Args:
            src_values(Iterable): a list/tuple of actual values, integer or double/float 
            signed(bool): indicating the holding register to be signed or unsigned
            display_format(DisplayFormat): data format applies to every value in src_values
            endianness(int): endianness applies to display_format
        Returns:
            results(tuple): a tuple of holding registers
                            each holding register is formatted by big endian
        """
        if not isinstance(src_values, list) and not isinstance(src_values, tuple):
            src_values = (src_values,)

        fmt = display_format.format
        count = len(src_values)

        # big endian is read friendly notation. eg: AB/ABCD/ABCDEFGH
        pack_data = struct.pack(">" + str(count) +fmt, *src_values)
        if display_format == FMT_SIGNED_WORD or display_format == FMT_UNSIGNED_WORD:           
            if endianness == BYTE_ORDER_BIG_ENDIAN or endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                return src_values
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN or endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_little_endian_2bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        elif display_format == FMT_SIGNED_2WORD or display_format == FMT_UNSIGNED_2WORD or display_format ==FMT_FLOAT_2WORD:
            if endianness == BYTE_ORDER_BIG_ENDIAN:
                ret = pack_data
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN:
                ret = _swap_little_endian_4bytes(pack_data)
            elif endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_byte_big_endian_4bytes(pack_data)
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                ret = _swap_byte_little_endian_4bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        elif display_format == FMT_SIGNED_4WORD or display_format == FMT_UNSIGNED_4WORD or display_format == FMT_DOUBLE_4WORD:
            if endianness == BYTE_ORDER_BIG_ENDIAN:
                ret = pack_data
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN:
                ret = _swap_little_endian_8bytes(pack_data)
            elif endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_byte_big_endian_8bytes(pack_data)
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                ret = _swap_byte_little_endian_8bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        else:
            raise NotImplementedError("display format {} is not supported".format(display_format))

        data_format = ">{}h".format(len(ret)//2) if signed else ">{}H".format(len(ret)//2)
        results = struct.unpack(data_format, ret)
        return results

    @staticmethod
    def unpack_holding_registers(holding_registers, signed, display_format, endianness):
        """
        unpack a tuple/list of holding registers comming from modbus read multiple holding registers 
        to actual values according to endianness and display_format

        Args:
            holding_registers(Iterable): a tuple/list of holding registers comming from modbus function: read multiple holding registers
                                    where each holding register is big endian based, namely is AB!
            signed(bool): indicating the holding register to be signed or unsigned
            display_format(DisplayFormat): 
            endianness(int): define the endianness of display_format
        Returns:
            results(tuple): a tuple of actual values(int/double/float), which can be used directly
        """
        if not isinstance(holding_registers, Iterable):
            holding_registers = (holding_registers,)

        count = len(holding_registers)

        data_format = ">{}h".format(count) if signed else ">{}H".format(count)
        pack_data = struct.pack(data_format, *holding_registers)

        if display_format == FMT_SIGNED_WORD or display_format == FMT_UNSIGNED_WORD:
            if endianness == BYTE_ORDER_BIG_ENDIAN or endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                ret = pack_data
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN or endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_little_endian_2bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        elif display_format == FMT_SIGNED_2WORD or display_format == FMT_UNSIGNED_2WORD or display_format == FMT_FLOAT_2WORD:
            if endianness == BYTE_ORDER_BIG_ENDIAN:
                ret = pack_data
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN:
                ret = _swap_little_endian_4bytes(pack_data)
            elif endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_byte_big_endian_4bytes(pack_data)
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                ret = _swap_byte_little_endian_4bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        elif display_format == FMT_SIGNED_4WORD or display_format == FMT_UNSIGNED_4WORD or display_format == FMT_DOUBLE_4WORD:
            if endianness == BYTE_ORDER_BIG_ENDIAN:
                ret = pack_data
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN:
                ret = _swap_little_endian_8bytes(pack_data)
            elif endianness == BYTE_ORDER_BIG_ENDIAN_SWAP:
                ret = _swap_byte_big_endian_8bytes(pack_data)
            elif endianness == BYTE_ORDER_LITTLE_ENDIAN_SWAP:
                ret = _swap_byte_little_endian_8bytes(pack_data)
            else:
                raise Exception("[Modbus] endian type {} is not supported".format(endianness))
        else:
            raise NotImplementedError("display format {} is not supported".format(display_format))
        # ret is big endian based after swapping
        results = struct.unpack(">{}{}".format(len(ret)//display_format.bytes, display_format.format), ret)
        return results

    def _write_holding_registers(self, address, signed, holding_register_values, slave_id):
        """
        write multiple holding registers
        """
        count = len(holding_register_values)
        # signed is consistent with PLC
        data_format = ">{}h".format(count) if signed else ">{}H".format(count)
        with self._client_lock:
            try:
                result = self._modbus_client.execute(slave=slave_id,
                                                     function_code=cst.WRITE_MULTIPLE_REGISTERS,
                                                     starting_address=address,
                                                     output_value=holding_register_values,
                                                     data_format=data_format)
            except Exception as e:
                result = None
                raise Exception("[Modbus-Error] can not write holding registers! the error is: {}\n---".format(e))
        return result

    def _read_holding_registers(self, address, signed, count, slave_id):
        """
        read holding registers by starting adress and count of holding registers
        the result is a tuple of holding registers, each is big endian based. eg: AB

        """
        # ">" is read friendly notation in byte order, that is AB
        # signed is consistent with PLC
        data_format = ">{}h".format(count) if signed else ">{}H".format(count)
        with self._client_lock:
            try:
                result = self._modbus_client.execute(slave=slave_id,
                                                     function_code=cst.READ_HOLDING_REGISTERS,
                                                     starting_address=address,
                                                     quantity_of_x=count, 
                                                     data_format=data_format)
            except Exception as e:
                result = None
                raise Exception("[Modbus-Error] can not read holding registers! the error is: {}\n---".format(e))
        return result

    def write_holding_registers(self, address, holding_register_values):
        """
        write a serial of modbus holding registers
        
        Args:
            address(int): starting address of holding registers
            holding_register_values(Iterable):
        Returns:
        """

        self._write_holding_registers(address, self.signed, holding_register_values, self.slave_id)

    def write_hr_commands(self, address, values, display_format=FMT_SIGNED_WORD, endianness=BYTE_ORDER_BIG_ENDIAN):
        """
        write a serial of actual values to modbus holding registers.
        eg: values=[1, 360, 179, 1234] 
        the values should be identical in display_format and endianness
        """
        holding_register_values = ModbusTcpClient.pack_values(values, self.signed, display_format, endianness)
        self.write_holding_registers(address, holding_register_values)

    def read_holding_registers(self, address, count=1):
        """
        read a serial of modbus holding registers
        each of the result holding registers is big endian based, that is AB

        Args:
            address(int): starting address of holding registers
            count(int): count number of holding registers holding to be read
        Returns:
            results(tuple): a tuple of holding registers
        """
        read_holding_registers = self._read_holding_registers(address, self.signed, count, self.slave_id)
        return read_holding_registers

    def read_hr_commands(self, address, count=1, display_format=FMT_SIGNED_WORD, endianness=BYTE_ORDER_BIG_ENDIAN):
        """
        read a serial of modbus holding regesiters and parse them to actual values
        actual values should be identical in display_format and endianness

        Args:
            address(int): starting address of holding registers
            count(int): count number of actual values
            display_format:
            endianness:
        Returns:
            a tuple of actual values(int/double/float), which can be used directly
        """
        words_num = count * (display_format.bytes//2)
        read_holding_registers = self.read_holding_registers(address, words_num)
        read_data = ModbusTcpClient.unpack_holding_registers(read_holding_registers, self.signed, display_format, endianness)
        return read_data

__all__ = [
    "ModbusTcpClient",
    "FMT_SIGNED_WORD", "FMT_UNSIGNED_WORD", "FMT_SIGNED_2WORD", "FMT_UNSIGNED_2WORD",
    "FMT_FLOAT_2WORD", "FMT_SIGNED_4WORD", "FMT_UNSIGNED_4WORD", "FMT_DOUBLE_4WORD",
    "BYTE_ORDER_BIG_ENDIAN", "BYTE_ORDER_LITTLE_ENDIAN", 
    "BYTE_ORDER_BIG_ENDIAN_SWAP", "BYTE_ORDER_LITTLE_ENDIAN_SWAP"]

if __name__ == "__main__":
    mc = ModbusTcpClient(server_ip="127.0.0.1", signed=True, port=502, slave_id=1, max_retry=5)
    connected = mc.connect()
    if not connected:
        print("make sure there is ModbusTcp slave")
        exit()

    l_signed_word = [-1, -32768, -1567, 0, 32766, 16524, 32767, 1, 4567]    
    mc.write_hr_commands(0, l_signed_word, display_format=FMT_SIGNED_WORD, endianness=BYTE_ORDER_BIG_ENDIAN) # endianness like Siemens PLC

    ret_data = mc.read_hr_commands(address=0, count=len(l_signed_word),\
                                   display_format=FMT_SIGNED_WORD, endianness=BYTE_ORDER_BIG_ENDIAN)
    print(ret_data)

    for i in range(len(l_signed_word)):
        if ret_data[i] != l_signed_word[i]: 
            print("test signed word failed")
            break
    print("test signed word ok")

    l_signed_dword = [-1, -123456789, -1567, 0, 123456789, 16524, 360123, -1456789,
                      -0, 1, 45678912, -45678912]    
    mc.write_hr_commands(0, l_signed_dword, display_format=FMT_SIGNED_2WORD, endianness=BYTE_ORDER_BIG_ENDIAN) 

    ret_data = mc.read_hr_commands(address=0, count=len(l_signed_dword), \
                                   display_format=FMT_SIGNED_2WORD, endianness=BYTE_ORDER_BIG_ENDIAN)
    print(ret_data)
    for i in range(len(l_signed_dword)):
        if ret_data[i] != l_signed_dword[i]: 
            print("test signed dword failed")
            break
    print("test signed dword ok")

    ret_float = mc.read_hr_commands(address=40, count=1, \
                                    display_format=FMT_FLOAT_2WORD, endianness=BYTE_ORDER_BIG_ENDIAN)
    print(ret_float)
    mc.close()
