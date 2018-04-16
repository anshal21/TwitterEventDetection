rm -rf Tweet
mkdir Tweet
cp $1 Tweet/Tweet
cd Tweet
split -100 Tweet
chmod 777 *
cd ..
cnt=0
for filename in ./Tweet/x*; do
	`python streamProcessor.py $filename ./Tweet/Processed$cnt`
	rm -rf $filename
	cnt=$((cnt+1))
done

cnt=0
for filename in ./Tweet/Processed*; do
	`python gen.py $filename ./Tweet/out$cnt`
	rm -rf $filename
	cnt=$((cnt+1))
done

for filename in ./Tweet/out*; do
	cat $filename >> ./Tweet/doc
	rm -rf $filename
done
pwd
cp ./Tweet/doc ./ProcessedTweets
rm -rf Tweet
