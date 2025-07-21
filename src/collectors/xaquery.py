import os
import win32com.client
import pythoncom
import threading
import pickle
from datetime import datetime, timedelta, time
import telegram
import math
import numpy as np


class XAQuery:
    CSPAQ12200_event = None
    CSPAQ12200_ok = False

    def OnReceiveData(self, szConde):
        if szConde == "CSPAQ12200":
            print("수신완료 %s" % szConde, flush=True)

            RecCnt = self.GetFieldData("CSPAQ12200OutBlock1", "RecCnt", 0)
            MgmtBrnNo = self.GetFieldData("CSPAQ12200OutBlock1", "MgmtBrnNo", 0)
            AcntNo = self.GetFieldData("CSPAQ12200OutBlock1", "AcntNo", 0)
            Pwd = self.GetFieldData("CSPAQ12200OutBlock1", "Pwd", 0)
            BalCreTp = self.GetFieldData("CSPAQ12200OutBlock1", "BalCreTp", 0)
            BrnNm = self.GetFieldData("CSPAQ12200OutBlock2", "BrnNm", 0)
            AcntNm = self.GetFieldData("CSPAQ12200OutBlock2", "AcntNm", 0)
            Dps = self.GetFieldData("CSPAQ12200OutBlock2", "Dps", 0)

            print("계좌번호: %s, 예수금: %s" % (AcntNo, Dps))

    def OnReceiveMessage(self, systemError, messegaCode, message):
        print("systemError: %s, messegaCode: %s, message: %s" % (systemError, messegaCode, message))