#!/usr/bin/env python

import sys

tally={}
for l in open(sys.argv[1]):
    if "d_dataset" in l: continue
    sp=l.split(',')
    dataset=sp[0]
    weight=float(sp[3])
    campaign = dataset.split('/')[2]
    if '-' in campaign:
        campaign = '-'.join(campaign.split('-')[0:-1])
    if len(campaign)<2:
        print("BUG1",dataset)
    sp=campaign.split('_')
 
    for i in range(4):
        if not sp[-(i+1)].startswith('v'):
            break
    if i>0 and '_' in campaign:
        campaign='_'.join(sp[:-i])
#    if "Run2016G" in campaign:
#        print("Strange",campaign,dataset)
    if len(campaign)<2:
        print("BUG",dataset)
    tally[campaign]=tally.get(campaign,0.)+weight

for t in tally:
    print("%7.2f %s"%(tally[t]/1.e12,t))
