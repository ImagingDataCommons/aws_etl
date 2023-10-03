bucket="s3://idc-open-data-logs"
months={5:10}

mlist=list(months.keys())
for mnth in mlist:
  numd= months[mnth]
  for i in range(1,numd+1):
    #print(str(mnth)+' '+str(i))
    src=bucket+'/2023-'+str(mnth).zfill(2)+'-'+str(i).zfill(2)+'*'
    dest=bucket+'/orig/'
    print("echo moving "+src)
    print("/home/ec2-user/s5cmd mv '"+src+"' '"+dest+"'")



