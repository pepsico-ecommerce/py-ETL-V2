from datetime import datetime
from datetime import timedelta 
import win32com.client

from absl import app
from absl import flags


FLAGS = flags.FLAGS

flags.DEFINE_string("job",
                    None,
                    "job name, currently supports " 
                    + "GECM_RefreshAmazonFreshPN"
                    #+ ", " + CUSTOMER_KROGER_PICKUP
                    )

flags.DEFINE_float("delay",
                    30,
                    "Delay minutes")

flags.DEFINE_bool("debug",
                    False,
                    "Run in debug mode")

# Required flag
flags.mark_flag_as_required("job")


def main(argv):
    del argv
    job = FLAGS.job
    delay = FLAGS.delay
    debug = FLAGS.debug
    #minutes = 30
    now = datetime.now() + timedelta(minutes=delay)
    # dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    dt_string = now.strftime("%Y-%m-%d")
    tm_string = now.strftime("%H:%M:%S")
    start = dt_string + ' ' + tm_string
    subject = "[Man File Load] Check SS Agent job '" + job + "'"
    #print(start, subject)
    #exit(1)
    addevent(start, subject)


def addevent(start, subject):
    # datetime object containing current date and time
    oOutlook = win32com.client.Dispatch("Outlook.Application")
    appointment = oOutlook.CreateItem(1) # 1=outlook appointment item
    appointment.Start = start
    appointment.Subject = subject
    appointment.Duration = 5
    appointment.Location = 'NA'
    appointment.ReminderSet = True
    appointment.ReminderMinutesBeforeStart = 1
    appointment.Save()


if __name__ == "__main__":
    app.run(main)
