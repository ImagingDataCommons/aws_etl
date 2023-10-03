
open IN, "two.lst";
while ($line=<IN>){
#print $line;
chomp($line);
@words=split(/\s+/,$line);
$ren=$words[3];
$ren=~ s/^orig//;
print "/Users/george/s5cmd/s5cmd mv s3://idc-open-data-two-logs/$words[3] s3://idc-open-data-two-logs/orig/$ren\n";
}
