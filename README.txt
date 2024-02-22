1)	Το olap_populate_sql_v23_Full που περιέχει όλους τους πίνακες εκτός από τους πίνακες της φάσης 2.
2)	Το olap_populate_sql_v23_Full_non_MC που περιέχει όλους τους πίνακες εκτός από τους πίνακες της φάσης 2 και τον πίνακα Γ κατηγορίας.
3)	Το  olap_populate_sql_uni_optimized που περιέχει τους πίνακες της φάσης 2 που έχει κάνει η unisystems.
4)	Και το olap_populate_sql_v23_Incremental που περιέχει ακριβώς τα ίδια στοιχεία με το olap_populate_sql_v23_Full  με λιγότερα σχόλια στο τέλος.
4) Η function olap_populate_Incremental δημιουργεί μία function με το αρχείο olap_populate_sql_v23_Incremental.
5) Η function olap_populate_uni δημιουργεί μία function με το αρχείο olap_populate_sql_uni_optimized
6) Τα αρχεια με V2 περιέχουν την προσθήκη ResponsibleOfficer
7) To αρχείο olap_populate_sql_uni_optimized_full είναι για αρχικοποίηση του olap_populate_sql_uni_optimized.
