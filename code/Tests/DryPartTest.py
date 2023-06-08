import unittest
import DryPartQuery
from DryPartQuery import AbstractTest
from operator import itemgetter


class DryPartTest(AbstractTest):
    def test_1(self):
        self.assertTrue(DryPartQuery.addGrade('Amir', 'DB', 80, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'DB', 81, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Dana', 'DB', 82, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Shir', 'DB', 82, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'DB', 83, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Noa', 'DB', 84, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'DB', 85, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Adam', 'DB', 86, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'OS', 80, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'OS', 81, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Amir', 'OS', 82, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'OS', 83, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'OS', 84, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Noa', 'OS', 85, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'PL', 80, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Yossi', 'PL', 81, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'PL', 82, 'Spring'))
        pairs = DryPartQuery.getQueryResult()

        # Uncomment the following lines to see the pairs your query returned
        print("\nPairs received in test 1:")
        print("   n1  |   n2  ")
        for pair in pairs:
            print(pair)
        self.assertEqual(2, len(pairs), "The query should return 2 pairs")
        sorted_pairs = sorted(pairs, key=itemgetter(0,1))

        self.assertTupleEqual(sorted_pairs[0], ('Amir', 'Avi'))				
        self.assertTupleEqual(sorted_pairs[1], ('Miki', 'Noa'))				


    def test_2(self):
        self.assertTrue(DryPartQuery.addGrade('Miki', 'DB', 84, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'DB', 84, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'DB', 85, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Yossi', 'DB', 86, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'DC', 80, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'DC', 82, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Amir', 'DC', 90, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Dana', 'DC', 91, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Noa', 'DC', 92, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'DC', 93, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'OS', 82, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'OS', 84, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Yossi', 'OS', 86, 'Summer'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'OS', 88, 'Summer'))
        pairs = DryPartQuery.getQueryResult()

        # Uncomment the following lines to see the pairs your query returned
        print("\nPairs received in test 2:")
        print("   n1  |   n2  ")
        for pair in pairs:
            print(pair)
        self.assertEqual(7, len(pairs), "The query should return 7 pairs")
        sorted_pairs = sorted(pairs, key=itemgetter(0,1))

        self.assertTupleEqual(sorted_pairs[0], ('Amir', 'Dana'))				
        self.assertTupleEqual(sorted_pairs[1], ('Amir', 'Noa'))				
        self.assertTupleEqual(sorted_pairs[2], ('Amir', 'Rotem'))				
        self.assertTupleEqual(sorted_pairs[3], ('Dana', 'Noa'))				
        self.assertTupleEqual(sorted_pairs[4], ('Dana', 'Rotem'))				
        self.assertTupleEqual(sorted_pairs[5], ('Miki', 'Avi'))				
        self.assertTupleEqual(sorted_pairs[6], ('Noa', 'Rotem'))				
	
    
    def test_3(self):
        self.assertTrue(DryPartQuery.addGrade('Avi', 'DB', 67, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Yossi', 'DB', 77, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Dana', 'DB', 86, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Amir', 'DB', 100, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'DB', 100, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'DB', 100, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'DS', 85, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'DS', 85, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Noa', 'DS', 90, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Rotem', 'DS', 97, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Yossi', 'OS', 84, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Dana', 'OS', 85, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Avi', 'OS', 90, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Miki', 'OS', 90, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Noa', 'OS', 91, 'Spring'))
        self.assertTrue(DryPartQuery.addGrade('Yuval', 'OS', 91, 'Winter'))
        self.assertTrue(DryPartQuery.addGrade('Amir', 'OS', 100, 'Winter'))
        pairs = DryPartQuery.getQueryResult()

        # Uncomment the following lines to see the pairs your query returned
        print("\nPairs received in test 3:")
        print("   n1  |   n2  ")
        for pair in pairs:
            print(pair)
        self.assertEqual(6, len(pairs), "The query should return 6 pairs")
        sorted_pairs = sorted(pairs, key=itemgetter(0,1))

        self.assertTupleEqual(sorted_pairs[0], ('Dana', 'Amir'))				
        self.assertTupleEqual(sorted_pairs[1], ('Dana', 'Yuval'))				
        self.assertTupleEqual(sorted_pairs[2], ('Miki', 'Noa'))				
        self.assertTupleEqual(sorted_pairs[3], ('Yossi', 'Amir'))				
        self.assertTupleEqual(sorted_pairs[4], ('Yossi', 'Dana'))				
        self.assertTupleEqual(sorted_pairs[5], ('Yossi', 'Yuval'))				



if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)

