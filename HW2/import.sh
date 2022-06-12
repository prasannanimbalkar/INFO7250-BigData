#!/bin/bash
  
for letter in {A..Z};
do
	fileLocation="/Users/prasannanimbalkar/Desktop/MSIS/BigData/HW2/NYSE/NYSE_daily_prices_$letter.csv"
	echo $fileLocation
	mongoimport --db=HW2-BigData --collection=nsye_stock_data --type=csv --file=$fileLocation --headerline

done  	
