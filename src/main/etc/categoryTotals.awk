BEGIN {
    FS=","
}
/^[0-9]/{
    if ($2 in categoryTotal) {
	categoryTotal[$2] += 1
    } else {
	categoryTotal[$2] = 1
    }
}
END {
    for (category in categoryTotal) {
	printf("%s,%d\n", category, categoryTotal[category])
    }
}