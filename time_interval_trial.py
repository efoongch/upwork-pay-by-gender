from apscheduler.schedulers.background import BlockingScheduler

sched = BlockingScheduler()

def some_job():
	print ("Every 10 seconds")

sched.add_job(some_job, 'interval', seconds=10)

print('Press Ctrl+C to exit')

try:
	sched.start()

except (KeyboardInterrupt, SystemExit):
	pass
