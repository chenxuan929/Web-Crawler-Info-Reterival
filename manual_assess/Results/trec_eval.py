import sys
import getopt
import math
import matplotlib.pyplot as plt
import os

class TrecEval:
    def __init__(self, qrel_file, trec_file, print_all_queries=False, graph=False):
        self.qrel = self.load_qrel(qrel_file)
        self.trec = self.load_trec(trec_file)
        self.cutoffs = [5, 10, 20, 50, 100]
        self.print_all_queries = print_all_queries
        self.graph = graph
        self.debug_relevant_docs()

    def load_qrel(self, filepath):
        qrel = {}
        try:
            with open(filepath, 'r') as file:
                for line in file:
                    query_id, _, doc_id, relevance = line.strip().split()
                    if query_id not in qrel:
                        qrel[query_id] = {}
                    qrel[query_id][doc_id] = int(relevance)
        except Exception as e:
            print(f"Error loading QREL file: {e}")
            sys.exit(1)
        return qrel
    
    def debug_relevant_docs(self):
        for query_id, docs in self.qrel.items():
            total_relevant = sum(1 for doc, rel in docs.items() if rel > 0)
            print(f"Query ID: {query_id}, Total Relevant Documents: {total_relevant}")

    def load_trec(self, filepath):
        trec = {}
        try:
            with open(filepath, 'r') as file:
                for line in file:
                    query_id, _, doc_id, _, score, _ = line.strip().split()
                    if query_id not in trec:
                        trec[query_id] = []
                    trec[query_id].append((doc_id, float(score)))
        except Exception as e:
            print(f"Error loading TREC file: {e}")
            sys.exit(1)
        return trec

    def calculate_metrics(self):
        metrics = {'average_precision': [], 'r_precision': [], 'ndcg': []}
        for cutoff in self.cutoffs:
            metrics[f'precision@{cutoff}'] = []
            metrics[f'recall@{cutoff}'] = []
            metrics[f'f1@{cutoff}'] = []

        for query_id in self.trec:
            ranked_docs = sorted(self.trec[query_id], key=lambda x: x[1], reverse=True)
            relevant_docs = self.qrel.get(query_id, {})
            num_relevant = sum(relevant_docs.values())

            precision_at_k = []
            recall_at_k = []
            num_rel_retrieved = 0

            for rank, (doc_id, _) in enumerate(ranked_docs, start=1):
                if doc_id in relevant_docs and relevant_docs[doc_id] > 0:
                    num_rel_retrieved += 1
                precision = num_rel_retrieved / rank
                recall = num_rel_retrieved / num_relevant if num_relevant > 0 else 0
                #print(recall, num_rel_retrieved, num_relevant)
                precision_at_k.append(precision)
                recall_at_k.append(recall)

                if rank in self.cutoffs:
                    metrics[f'precision@{rank}'].append(precision)
                    metrics[f'recall@{rank}'].append(recall)
                    if precision + recall > 0:
                        metrics[f'f1@{rank}'].append(2 * precision * recall / (precision + recall))
                    else:
                        metrics[f'f1@{rank}'].append(0)

            average_precision = sum(precision_at_k[i] for i, doc_id in enumerate(ranked_docs) if doc_id[0] in relevant_docs and relevant_docs[doc_id[0]] > 0) / num_relevant if num_relevant > 0 else 0
            metrics['average_precision'].append(average_precision)

            r_precision = precision_at_k[num_relevant - 1] if num_relevant <= len(ranked_docs) else num_rel_retrieved / num_relevant
            metrics['r_precision'].append(r_precision)

            dcg = sum((2 ** relevant_docs.get(doc_id, 0) - 1) / math.log2(rank + 1) for rank, (doc_id, _) in enumerate(ranked_docs[:num_relevant], start=1))
            sorted_relevances = sorted(relevant_docs.values(), reverse=True)
            idcg = sum((2 ** rel - 1) / math.log2(rank + 1) for rank, rel in enumerate(sorted_relevances, start=1))
            ndcg = dcg / idcg if idcg > 0 else 0
            metrics['ndcg'].append(ndcg)

            if self.print_all_queries:
                print(f"Query ID: {query_id}")
                print(f"{'Precision@5:':<20}{metrics['precision@5'][-1]:>10.4f}")
                print(f"{'Recall@5:':<20}{metrics['recall@5'][-1]:>10.4f}")
                print(f"{'F1@5:':<20}{metrics['f1@5'][-1]:>10.4f}")
                print(f"{'Precision@10:':<20}{metrics['precision@10'][-1]:>10.4f}")
                print(f"{'Recall@10:':<20}{metrics['recall@10'][-1]:>10.4f}")
                print(f"{'F1@10:':<20}{metrics['f1@10'][-1]:>10.4f}")
                print(f"{'Precision@20:':<20}{metrics['precision@20'][-1]:>10.4f}")
                print(f"{'Recall@20:':<20}{metrics['recall@20'][-1]:>10.4f}")
                print(f"{'F1@20:':<20}{metrics['f1@20'][-1]:>10.4f}")
                print(f"{'Precision@50:':<20}{metrics['precision@50'][-1]:>10.4f}")
                print(f"{'Recall@50:':<20}{metrics['recall@50'][-1]:>10.4f}")
                print(f"{'F1@50:':<20}{metrics['f1@50'][-1]:>10.4f}")
                print(f"{'Precision@100:':<20}{metrics['precision@100'][-1]:>10.4f}")
                print(f"{'Recall@100:':<20}{metrics['recall@100'][-1]:>10.4f}")
                print(f"{'F1@100:':<20}{metrics['f1@100'][-1]:>10.4f}")
                print("\nAverage Precision:      {:>10.4f}".format(metrics['average_precision'][-1]))
                print(f"R-Precision:            {metrics['r_precision'][-1]:>10.4f}")
                print(f"nDCG:                   {metrics['ndcg'][-1]:>10.4f}")
                print("-" * 40)

        for metric, values in metrics.items():
            if values:
                label = f"Mean {metric}:" 
                value = f"{sum(values) / len(values):.4f}"
                print(f"{label:<20}{value:>10}")

        if self.graph:
            plt.plot(recall_at_k, precision_at_k, marker='.')
            plt.xlabel('Recall')
            plt.ylabel('Precision')
            plt.title('Precision-Recall Curve')
            plt.savefig('precision_recall_curve.png')
            plt.show()



def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hqg", ["help", "query-level", "graph"])
    except getopt.GetoptError:
        print('Usage: trec_eval.py -q -g <qrel_file> <trec_file>')
        sys.exit(2)

    print_all_queries = False
    graph = False
    for opt, arg in opts:
        if opt == '-h':
            print('Usage: trec_eval.py -q -g <qrel_file> <trec_file>')
            sys.exit()
        elif opt in ("-q", "--query-level"):
            print_all_queries = True
        elif opt in ("-g", "--graph"):
            graph = True

    if len(args) != 2:
        print('Usage: trec_eval.py -q -g <qrel_file> <trec_file>')
        sys.exit(2)

    qrel_file, trec_file = args
    evaluator = TrecEval(qrel_file, trec_file, print_all_queries, graph)
    evaluator.calculate_metrics()




def read_precision(filepath):
    precisions = []
    recalls = []
    with open(filepath, 'r') as file:
        for line in file:
            if 'Precision@' in line:
                value = line.strip().split(':')[-1].strip()
                precisions.append(float(value))
            elif 'Recall@' in line:
                value = line.strip().split(':')[-1].strip()
                recalls.append(float(value))
    return precisions, recalls

def plot_precision_recall(results_directory):
    for filename in os.listdir(results_directory):
        if filename.endswith("_results.txt"):
            filepath = os.path.join(results_directory, filename)
            query_id = filename.split('_')[1]
            precisions, recalls = read_precision(filepath)

            plt.figure(figsize=(8, 5))
            plt.plot(recalls, precisions, marker='o', linestyle='-', label=f'Query {query_id}')
            plt.xlabel('Recall')
            plt.ylabel('Precision')
            plt.title(f'Precision-Recall Curve for Query {query_id}')
            plt.legend()
            plt.grid(True)
            #plt.show()
            save_filename = f'Curve_{query_id}.png'
            plt.savefig(save_filename)
            plt.close()




if __name__ == "__main__":
    main(sys.argv[1:])
    #plot_precision_recall('Results/trec_results/')

# python trec_eval.py -q -g <path_to_qrel_file> <path_to_trec_file>
# python trec_eval.py -q qrels.adhoc.51-100.AP89.txt ES_builtin_output.txt

# python trec_eval.py -q assessments_04.txt ranked_04.txt
# python trec_eval.py -q assessments.qrel ranked_list.txt 