import operator, os, sys
import time
import numpy as np
from mpi4py import MPI
comm = MPI.COMM_WORLD
root = 0
rank = comm.rank
size = comm.size
sendFiles = []

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


def getTerms(files, dataSet):
    sendFiles = comm.bcast(files, root)
    mainTerms = []
    for i in range(rank, len(sendFiles), size):
        file = open(dataSet + sendFiles[i], 'r')
        termsDoc = {}
        for line in file:
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
        file.close()

        for j in range(10):
            mainTerms.append(termsDocSort[j][0])
    recivFiles = comm.gather(mainTerms, root)
    return recivFiles, sendFiles


def countTerms(terms, sendFiles):
    sendTerms = comm.bcast(terms, root)
    #print("RANK: ", comm.rank, "sendTerms: ", sendTerms)
    counter = {}
    for i in range(rank, len(sendFiles), size):
        resultDoc = []
        for j in range(len(sendTerms)):
            resultDoc.append(0)
        file = open(dataSet + sendFiles[i], 'r')
        for line in file:
            line = line.lower().replace(',', '').replace(';', '').replace('.', '').replace(':', '').replace('-','').replace('\"', '').replace('(', '').replace(')', '').replace('[', '').replace(']', '')
            for word in line.split():
                word.strip()
                if word not in stopwordsman:
                    if word in sendTerms:
                        resultDoc[sendTerms.index(word)] += 1
        counter[sendFiles[i]] = resultDoc
    recivTerms = comm.gather(counter, root)
    return recivTerms


def calcDistances(counter):
    sendCounter = comm.bcast(counter, root)

    tam = len(sendCounter)
    distances = np.zeros((tam,tam))
    files = list(sendCounter.keys())
    for i in range(rank, len(sendCounter), size):
        for j in range(tam):
            distances[i][j] = 1.0 - jaccard(sendCounter[files[i]], sendCounter[files[j]])
    recivCounter = comm.gather(distances, root)
    return recivCounter, sendCounter


#Tomado de http://dataconomy.com/2015/04/implementing-the-five-most-popular-similarity-measures-in-python/
def jaccard(x, y):
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality / float(union_cardinality)


def kMeans(distances, centroids, maxIters, k):
    sendAssign = []
    for i in range(maxIters):
        sendDistances = comm.bcast(distances, root)
        sendCentroids = comm.bcast(centroids, root)
        tam = len(sendDistances)
        argminlist = np.zeros(tam)
        for j in range(rank, len(sendDistances), size):
            dotList = []
            for centroid in sendCentroids:
                dotList.append(np.dot(sendDistances[j]-centroid, sendDistances[j]- centroid))
            argminlist[j] = np.argmin(dotList)
        recivAssignTemp = comm.gather(argminlist, root)

        assign = []
        if rank == 0:
            assign = np.zeros(len(recivAssignTemp[0]))
            for j in range(len(recivAssignTemp)):
                assign += recivAssignTemp[j]
        sendAssign = comm.bcast(assign, root)
        centroidesTemp = []

        for j in range(k):
            centroidesTemp.insert(i, [])

        for j in range(rank, k, size):
            trueFalseArr = sendAssign == j
            docPropios = sendDistances[trueFalseArr]
            averages = docPropios.mean(axis=0)
            centroidesTemp[j] = list(averages)

        recivCentroides = comm.gather(centroidesTemp, root)
        centroidsAux = []
        for j in range(k):
            centroidsAux.append([])
        if rank == root:
            for j in range(len(recivCentroides)):
                for jj in range(len(recivCentroides[j])):
                    centroidsAux[jj] += recivCentroides[j][jj]
            centroids = centroidsAux
    #Revisar
    return sendAssign


if __name__ == '__main__':
    timeini = time.time()
    k = int(sys.argv[2])
    maxIters = 10
    dataSet = sys.argv[1]

    files = []
    if rank == root:
        files = list(os.walk(dataSet))[0][2]

    termsTemp, sendFiles = getTerms(files, dataSet)

    terms = []
    if rank == root:
        for i in range(len(termsTemp)):
            terms.extend([element for element in termsTemp[i] if element not in terms])

    counterTemp = countTerms(terms, sendFiles)
    counter = {}
    if rank == root:
        for i in range(len(counterTemp)):
            counter.update(counterTemp[i])


    distancesTemp, sendCounter = calcDistances(counter)

    #print("------RecibMatrixC: ",distancesTemp)
    centroids = []
    distances = 0
    if rank == root:
        for eachMatrix in distancesTemp:
            distances += eachMatrix
        #print(distances)
        #centroids = distances[np.random.choice(np.arange(len(distances)), k), :]
        centroids = distances[np.random.choice(np.arange(len(distances)), k), :]

    assignTemp = kMeans(distances, centroids, maxIters, k)

    if rank == root:
        timetotal = time.time()-timeini
        #print(timetotal)
        clustering = []
        for i in range(k):
            clustering.insert(i, [])

        files = list(sendCounter.keys())
        count = 0
        for i in assignTemp:
            clustering[int(i)].append(files[count])
            count +=1

        #print(clustering)
        dataset2 = dataSet.replace("./", "").replace("/", "")
        salida = "salidaParalelo"+dataset2+"K"+str(k)+".txt"
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
