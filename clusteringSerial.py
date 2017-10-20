import operator, os, sys
import numpy as np
import time


def getTerms(dataSet):
    stopwordsman = ["a", "able", "about", "above", "according", "accordingly", "across", "actually", "after",
                    "afterwards", "again", "against", "all", "allow", "allows", "almost", "alone", "along",
                    "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another",
                    "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart",
                    "appear", "appreciate", "appropriate", "are", "around", "as", "aside", "ask", "asking",
                    "associated", "at", "available", "away", "awfully", "b", "be", "became", "because", "become",
                    "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below",
                    "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "c",
                    "came", "can", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes",
                    "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider",
                    "considering", "contain", "containing", "contains", "corresponding", "could", "course",
                    "currently", "d", "definitely", "described", "despite", "did", "different", "do", "does",
                    "doing", "done", "down", "downwards", "during", "e", "each", "edu", "eg", "eight", "either",
                    "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every",
                    "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "f",
                    "far", "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former",
                    "formerly", "forth", "four", "from", "further", "furthermore", "g", "get", "gets", "getting",
                    "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "h", "had",
                    "happens", "hardly", "has", "have", "having", "he", "hello", "help", "hence", "her", "here",
                    "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his",
                    "hither", "hopefully", "how", "howbeit", "however", "i", "ie", "if", "ignored", "immediate",
                    "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar",
                    "instead", "into", "inward", "is", "it", "its", "itself", "j", "just", "k", "keep", "keeps",
                    "kept", "know", "knows", "known", "l", "last", "lately", "later", "latter", "latterly",
                    "least", "less", "lest", "let", "like", "liked", "likely", "little", "ll", "look", "looking",
                    "looks", "ltd", "m", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely",
                    "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "n", "name",
                    "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never",
                    "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor",
                    "normally", "not", "nothing", "novel", "now", "nowhere", "o", "obviously", "of", "off",
                    "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other",
                    "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over",
                    "overall", "own", "p", "particular", "particularly", "per", "perhaps", "placed", "please",
                    "plus", "possible", "presumably", "probably", "provides", "q", "que", "quite", "qv", "r",
                    "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards",
                    "relatively", "respectively", "right", "s", "said", "same", "saw", "say", "saying", "says",
                    "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self",
                    "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she",
                    "should", "since", "six", "so", "some", "somebody", "somehow", "someone", "something",
                    "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify",
                    "specifying", "still", "sub", "such", "sup", "sure", "t", "take", "taken", "tell", "tends",
                    "th", "than", "thank", "thanks", "thanx", "that", "thats", "the", "their", "theirs", "them",
                    "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein",
                    "theres", "thereupon", "these", "they", "think", "third", "this", "thorough", "thoroughly",
                    "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too",
                    "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "u",
                    "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us",
                    "use", "used", "useful", "uses", "using", "usually", "uucp", "v", "value", "various", "ve",
                    "very", "via", "viz", "vs", "w", "want", "wants", "was", "way", "we", "welcome", "well",
                    "went", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
                    "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
                    "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish",
                    "with", "within", "without", "wonder", "would", "would", "x", "y", "yes", "yet", "you", "your",
                    "yours", "yourself", "yourselves", "z", "zero"]
    terms = []
    for file in list(os.walk(dataSet))[0][2]:
        text = open(dataSet + file, 'r')
        termsDoc = {}
        for line in text:
            line = line.lower().replace(',', '').replace(';', '').replace('.', '').replace(':', '').replace('-','').replace('\"', '').replace('(', '').replace(')', '').replace('[', '').replace(']', '')
            for word in line.split():
                word.strip()

                if word not in stopwordsman:
                    if word in termsDoc and word != "":
                        termsDoc[word] += 1
                    else:
                        termsDoc[word] = 1
        termsDocSort = sorted(termsDoc.items(), key=operator.itemgetter(1))
        termsDocSort = termsDocSort[::-1]
        #file.close()
        mainTerms = []
        for i in range(10):
            mainTerms.append(termsDocSort[i][0])
        terms = list(set(terms + mainTerms))
    return terms

def countTerms(terms):
    counter = {}
    for file in list(os.walk(dataSet))[0][2]:
        resultDoc = []
        for i in range(len(terms)):
            resultDoc.append(0)
        text = open(dataSet + file, "r")
        for line in text:
            line = line.lower().replace(',', '').replace(';', '').replace('.', '').replace(':', '').replace('-','').replace('\"', '').replace('(', '').replace(')', '').replace('[', '').replace(']', '')
            for word in line.split():
                word.strip()

                if word in terms:
                    resultDoc[terms.index(word)] += 1

        counter[file] = resultDoc
    return counter

def calculateDistances(counter):
    n = len(counter)
    distances = np.empty((n, n))
    files = list(counter.keys())
    for i in range(n):
        for j in range(n):
            distances[i][j] = 1.0 - (jaccard(counter[files[i]], counter[files[j]]))
    # print(distances)
    return distances

#Tomado de http://dataconomy.com/2015/04/implementing-the-five-most-popular-similarity-measures-in-python/
def jaccard(x, y):
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality / float(union_cardinality)

def kMeans(X, K, maxIters=10):
    centroids = distances[np.random.choice(np.arange(len(distances)), k), :]
    C = []
    for i in range(maxIters):
        argminList = []
        for row in distances:
            dotList = []
            for centroid in centroids:
                dotList.append(np.dot(row - centroid, row - centroid))
            argminList.append(np.argmin(dotList))
        C = np.array(argminList)
        centroidesTemp = []
        for ki in range(k):
            truefalseArr = C == ki
            propiosKArr = distances[truefalseArr]
            promedioArr = propiosKArr.mean(axis=0)
            centroidesTemp.append(promedioArr)
        centroids = centroidesTemp

    return np.array(centroids), C

if __name__ == '__main__':
    timeini = time.time()
    k = int(sys.argv[2])
    dataSet = sys.argv[1]
    terms = getTerms(dataSet)
    #print("TERMS: ",terms)
    counter = countTerms(terms)
    #print("COUNTER: ", counter)
    distances = calculateDistances(counter)
    #print("DISTANCES: ", distances)
    centroids, assign = kMeans(distances, k)
    #print("CENTROIDS: ", centroids)
    #print("ASSIGN: ", assign)

    #Clusters
    timetotal = time.time()-timeini
    #print(timetotal)
    clustering = []
    for i in range(k):
        clustering.insert(i, [])

    files = list(counter.keys())
    count = 0
    for i in assign:
        clustering[i].append(files[count])
        count +=1

    #print(clustering)
    dataset2 = dataSet.replace("./", "").replace("/", "")
    salida = "salidaSerial"+dataset2+"K"+str(k)+".txt"
    file = open(salida, "w")
    file.write("Tiempo total: %f" %(timetotal))
    file.write("\n")
    for i in range(len(clustering)):
        file.write("Cluster %d :" % (i))
        file.write("\n")
        file.write(str(clustering[i]))
        file.write("\n")
        file.write("-"*10)
        file.write("\n")
    file.close()
    print("Archivo creado exitosamente: %s" %(salida))
