import sys
from ringinfoclient.ringmanagement import RingManagement


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print """
                                        Invalid parameters
                 --------------------------------------------------------------------
                 Parameters format:  IP       Account_name Container_name Object_name
                          e.g.   10.23.73.153        a            b           c
                 --------------------------------------------------------------------"""
        sys.exit()
    IP = sys.argv[1]
    ring = RingManagement(IP)
    account_name, container_name, object_name, file_path = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]

    # container_info = ring.put_Container(account_name, container_name)
    # print ring.put_Object(account_name, container_name, object_name, headers=container_info)
    print ring.put_object(account_name, container_name, object_name, file_path)