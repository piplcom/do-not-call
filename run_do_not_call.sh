#!/bin/sh
java -Xms30G -Xmx30G -cp do-not-call.jar pipl.pse.donotcallgen.DoNotCallGenerator workFolder=. inputFileName=2020-2-6_Global_4B95655D-7BFA-4CCA-8949-E911404284EE.txt doNotCallDate=2018_02_20
