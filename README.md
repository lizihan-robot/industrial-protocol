# add modbus rtu
ubuntu 先打开串口 sudo chmod 777 /dev/ttyS* 
	ttyS*表示串口
	ttyUSB* 表示usb接口
一次性永久打开串口 
	sudo gedit /etc/udev/rules.d/70-ttyusb.rules
	添加KERNEL=="ttyUSB[0-9]*", MODE="0666"
	

# add modbus tcp

salve 需要与PLC的一致 并且不重复，

# pymcprotocol

python 与三菱PLC mc通讯协议

参考链接
https://blog.csdn.net/cmwanysys/article/details/106681255
PLC需要选择允许RUN中写入（FTP与MC地址）

