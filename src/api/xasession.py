class XASession:
    login_ok = False

    def OnLogin(self, szCode, szMsg):
        print("로그인 상태:", szCode, szMsg)
        if szCode == "0000":
            XASession.login_ok = True
        else:
            XASession.login_ok = False
