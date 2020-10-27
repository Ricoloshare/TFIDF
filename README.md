# Use Hadoop to calculate TF*IDF

## TF

TF is word frequency.

We have two files.

![](https://github.com/Ricoloshare/TFIDF/tree/master/img/twoFile.png)

The word **hello** appears in 1.txt for 2 times. So his TF is 2 / 6(total words) = 0.33333

## IDF

IDF is inverse document frequency. 

![](https://github.com/Ricoloshare/TFIDF/tree/master/img/IDF.png)

The word **hello** appears in 1.txt. Similarly, it also appears in 2.txt.

So his IDF is log(2/(2+1)) = log(2/3)= -0.176

So his **TF*IDF** = 0.33333 * -0.176 = -0.058608

## Program running results

**TF results:**

![](https://github.com/Ricoloshare/TFIDF/tree/master/img/TF-result.png)

We can see the TF value of the word hello.

**TF*IDF**

![](https://github.com/Ricoloshare/TFIDF/tree/master/img/TFIDF.png)

The operation result is correct, let's take a look at the program.

## Algorithm

![](https://github.com/Ricoloshare/TFIDF/tree/master/img/sl.png)

*The document input for the second time is the first output*

### First Time(job1)

#### Map

```java

StringTokenizer words = new StringTokenizer(value.toString());
		
// get k2(fileName: word)   v2(v2 = 1)
while(words.hasMoreTokens()) {
	text.set(String.join(":", fileName, words.nextToken()));
	context.write(text, intWrite);
		
}
```

#### Reduce

``` java
//sum
if(values == null) {
    return;
}
sum = 0;
for(IntWritable value: values) {
    sum += value.get();	  // the total number of the same word
}
// use HashMap(key: fileName, value: sum)
//to count the total number of the document
String docuName = key.toString().split(":")[0];
if(docuMap.containsKey(docuName)) {
    docuMap.put(docuName, sum + docuMap.get(docuName));
}else {
    docuMap.put(docuName, sum);
}

keyMap.put(key.toString(), sum); //Save the total number of the same word
```

**The finally**

```java
for(Map.Entry<String, Integer> entry: keyMap.entrySet()) {
    String fileName = entry.getKey().split(":")[0];       // get HashMap key 
    tf = 1.0 * entry.getValue() / docuMap.get(fileName);  // Calculation TF value
    context.write(new Text(entry.getKey()), new Text(String.valueOf(tf.isInfinite() ? Double.MIN_VALUE : tf)));
}
context.write(new Text("totalDocu"),new Text(docuMap.size()+""));
```

### Second Time(job 2)

#### Map

``` java
// get words
StringTokenizer words = new StringTokenizer(value.toString());
String word = words.nextToken();

if(word.indexOf(":") == -1) { 
    totalFile = Integer.parseInt(words.nextToken());   //get the total number of document
}else {
    //get Key(word) value: tf: filName
    String tf = words.nextToken();
    textKay.set(word.split(":")[1]);
    textValue.set(String.join(":", tf, word.split(":")[0])); // word tf filename
    context.write(textKay, textValue); 
}
```

**Reduce**

```java
for(Text value: values) {
    lists.add(value.toString());
    fileCount++;
}//Count the total number of documents containing the word

// Calculation IDF value
Double idfValue = Math.log10(1.0 * TFIDFMapper.totalFile / (fileCount + 1));
Double tfidfValue;
Text text = new Text();
//Calculation TF*IDF value
for(String value : lists) {
    tfidfValue = idfValue * Double.parseDouble(value.split(":")[0]);
    text.set(String.join(":", value.split(":")[1],key.toString()));
    context.write(text, new Text(tfidfValue.toString()));
}
```

## Run in Hapdoop cluster

```shell
hadoop jar 1-0.0.1-SNAPSHOT.jar com.ricolo.TFIDFDriver
```

![](https://github.com/Ricoloshare/TFIDF/blob/master/img/run.png)

Download file from HDFS

![](https://github.com/Ricoloshare/TFIDF/blob/master/img/result.png)