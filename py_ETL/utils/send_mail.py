import win32com.client
outlook = win32com.client.Dispatch("Outlook.Application")

def sendEmail():
  body = """
    <html><body>
    <table border = 1>
    <tr>
    <th>JobName</th> <th>LastRunDateTime</th> <th>LastRunStatus</th> <th>LastRunDuration</th> 
    </tr>
    """

  xml = '<td>job 1</td> <td>2020-07-01 18:44:23</td> <td>Success</td> <td>1m 30s</td>'
  body += xml +'</table></body></html>'

  Msg = outlook.CreateItem(0) # Email
  Msg.To = "David.McIntyre.Contractor@pepsico.com" # you can add multiple emails with the ; as delimiter. E.g. test@test.com; test2@test.com;
  #Msg.CC = "test@test.com"
  Msg.Subject = "Mail Test"
  Msg.Body = "Hola amigo!"
  #Msg.Body = body
  #Msg.BodyFormat = 2
  Msg.HTMLBody = body
  Msg.display()
  #Msg.Send()


if __name__ == "__main__":
  sendEmail()