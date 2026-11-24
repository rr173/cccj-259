import unittest

from feedtable.intervals import Interval, subtract


class IntervalTests(unittest.TestCase):
    def test_validation(self):
        self.assertEqual((Interval(3, 7).lo, Interval(3, 7).hi), (3, 7))
        Interval(5, 5)  # empty is allowed
        with self.assertRaises(ValueError):
            Interval(7, 3)
        with self.assertRaises(TypeError):
            Interval(1.5, 3)
        with self.assertRaises(TypeError):
            Interval(0, True)

    def test_half_open(self):
        iv = Interval(0, 10)
        self.assertTrue(iv.contains(0))
        self.assertTrue(iv.contains(9))
        self.assertFalse(iv.contains(10))
        self.assertFalse(iv.contains(-1))

    def test_subtract_no_overlap(self):
        self.assertEqual(subtract(Interval(0, 5), Interval(5, 10)), [Interval(0, 5)])
        self.assertEqual(subtract(Interval(10, 15), Interval(5, 10)), [Interval(10, 15)])

    def test_subtract_contained(self):
        self.assertEqual(subtract(Interval(0, 10), Interval(-5, 20)), [])
        self.assertEqual(subtract(Interval(0, 10), Interval(2, 8)),
                         [Interval(0, 2), Interval(8, 10)])

    def test_subtract_shift_right(self):
        self.assertEqual(subtract(Interval(100, 130), Interval(0, 100)),
                         [Interval(100, 130)])

    def test_subtract_partial(self):
        # old [0,100) -> new [80,130): added [100,130), removed [0,80)
        old, new = Interval(0, 100), Interval(80, 130)
        self.assertEqual(subtract(new, old), [Interval(100, 130)])
        self.assertEqual(subtract(old, new), [Interval(0, 80)])

    def test_segments_are_disjoint_and_inside(self):
        old, new = Interval(10, 90), Interval(40, 160)
        segs = subtract(new, old) + subtract(old, new)
        for a in range(len(segs)):
            for b in range(a + 1, len(segs)):
                self.assertTrue(segs[a].hi <= segs[b].lo or segs[b].hi <= segs[a].lo)


if __name__ == "__main__":
    unittest.main()
