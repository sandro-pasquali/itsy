//Just count 0 to 2^n - 1 and print the numbers according to the binary //representation of your count.

var alph = ['a','b','c'];
var len = alph.length;
var count = Math.pow(2, alph.length); // -1 effected in for loop (< len)

for(var x=0; x < count; x++) {
	var str = "";
	for(var y=0; y < len; y++) {
		if(x & Math.pow(2, y)) {
			str += alph[y];
		} 
	}
	console.log(str);
}
