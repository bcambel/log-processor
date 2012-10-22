#! /usr/bin/awk -f
BEGIN {
	FPAT = "/some_pattern/"
}
{
	count += 1
	# printf ("%d" , ($4 == 4069))
	if ($4 == 4069){
	# 
	# printf( "%s %s ", $1, $3 )
	str = ""
	for (i = 15; i < NF - 1; i++) {
		str = str $i " "
	    #printf("$%d = <%s>\n", i, $i)
	}
	# cleaned = substr(str,2, (length(str)-4))
	# temp = "echo \"" cleaned "\" | openssl md5 | cut -f2 -d \" \""
	# temp | getline md5
	# close(temp)
	printf("%s %s %s \n" , $1, $3,str)
	}

} END { print count }