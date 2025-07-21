import win32com.client
from datetime import datetime


class XAQuery:
    CSPAQ12200_event = None
    CSPAQ12200_ok = False

    t1537_event = None
    t1537_ok = False
    t1537_dict = {}

    t8425_event = None
    t8425_ok = False
    t8425_dict = {}

    t1516_event = None
    t1516_ok = False

    def OnReceiveData(self, szCode):
        if szCode == "t8425":
            cnt = self.GetBlockCount("t8425OutBlock")
            for i in range(cnt):
                tmname = self.GetFieldData("t8425OutBlock", "tmname", i)
                tmcode = self.GetFieldData("t8425OutBlock", "tmcode", i)

                print(tmcode)
                # if tmcode not in XAQuery.t8425_dict:
                #     XAQuery.t8425_dict[tmcode] = {'테마이름': tmname}
                #     print(f"테마이름: {tmname}")

        elif szCode == "t1537":
            cnt = self.GetBlockCount("t1537OutBlock1")
            for i in range(cnt):
                hname = self.GetFieldData("t1537OutBlock1", "hname", i)
                price = self.GetFieldData("t1537OutBlock1", "price", i)
                shcode = self.GetFieldData("t1537OutBlock1", "shcode", i)
                XAQuery.t1537_dict[shcode] = {
                    'hname': hname,
                    'price': price
                }
    print(t8425_dict)

    def GetFieldData(self, *args):
        return "mock"

    def GetBlockCount(self, *args):
        return 2

    def IsNext(self):
        return False
