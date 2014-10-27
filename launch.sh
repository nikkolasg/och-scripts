
#!/bin/bash
#./monitoring.rb reset cdr flow mss --direction both
#./monitoring.rb reset records flow mss --direction both
./monitoring.rb get flow mss --direction both
./monitoring.rb insert flow mss --direction both
./monitoring.rb stats flow mss
