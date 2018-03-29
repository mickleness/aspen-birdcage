package org.abc.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

public class GroupedMapIteratorTest extends TestCase {

	@Test
	public void testIterator_2_2() {
		testIterator_2_2("10", "11");
		testIterator_3_1("10", "11");
		testIterator_1_3("10", "11");
		testIterator_4_0("10");
	}

	@Test
	public void testIterator_null() {
		testIterator_2_2(null, "11");
		testIterator_3_1(null, "11");
		testIterator_1_3(null, "11");
		testIterator_4_0(null);

		testIterator_2_2("10", null);
		testIterator_3_1("10", null);
		testIterator_1_3("10", null);
	}

	@Test
	public void testUnorderedMaps() {
		Map m1 = createMap("name", "Reshma", "grade", "10");
		Map m2 = createMap("name", "Sajith", "grade", "11");
		Map m3 = createMap("name", "Stephania", "grade", "10");
		Map m4 = createMap("name", "Gloria", "grade", "11");
		List<Map> srcList = Arrays.asList(m1, m2, m3, m4);
		GroupedMapIterator iter = new GroupedMapIterator(srcList.iterator(),
				"grade");
		try {
			while (iter.hasNext()) {
				iter.next();
			}
			fail("this should fail because the maps aren't sorted by grade");
		} catch (Exception e) {
			// pass!
		}
	}

	protected void testIterator_2_2(String grade1, String grade2) {
		Map m1 = createMap("name", "Reshma", "grade", grade1);
		Map m2 = createMap("name", "Sajith", "grade", grade1);
		Map m3 = createMap("name", "Stephania", "grade", grade2);
		Map m4 = createMap("name", "Gloria", "grade", grade2);
		List<Map> srcList = Arrays.asList(m1, m2, m3, m4);
		GroupedMapIterator iter = new GroupedMapIterator(srcList.iterator(),
				"grade");
		assertTrue(iter.hasNext());
		List<Map> r1 = iter.next();
		assertEquals(2, r1.size());
		assertEquals(m1, r1.get(0));
		assertEquals(m2, r1.get(1));

		assertTrue(iter.hasNext());
		List<Map> r2 = iter.next();
		assertEquals(2, r2.size());
		assertEquals(m3, r2.get(0));
		assertEquals(m4, r2.get(1));

		assertFalse(iter.hasNext());
	}

	public void testIterator_3_1(String grade1, String grade2) {
		Map m1 = createMap("name", "Reshma", "grade", grade1);
		Map m2 = createMap("name", "Sajith", "grade", grade1);
		Map m3 = createMap("name", "Stephania", "grade", grade1);
		Map m4 = createMap("name", "Gloria", "grade", grade2);
		List<Map> srcList = Arrays.asList(m1, m2, m3, m4);
		GroupedMapIterator iter = new GroupedMapIterator(srcList.iterator(),
				"grade");
		assertTrue(iter.hasNext());
		List<Map> r1 = iter.next();
		assertEquals(3, r1.size());
		assertEquals(m1, r1.get(0));
		assertEquals(m2, r1.get(1));
		assertEquals(m3, r1.get(2));

		assertTrue(iter.hasNext());
		List<Map> r2 = iter.next();
		assertEquals(1, r2.size());
		assertEquals(m4, r2.get(0));

		assertFalse(iter.hasNext());
	}

	public void testIterator_4_0(String grade) {
		Map m1 = createMap("name", "Reshma", "grade", grade);
		Map m2 = createMap("name", "Sajith", "grade", grade);
		Map m3 = createMap("name", "Stephania", "grade", grade);
		Map m4 = createMap("name", "Gloria", "grade", grade);
		List<Map> srcList = Arrays.asList(m1, m2, m3, m4);
		GroupedMapIterator iter = new GroupedMapIterator(srcList.iterator(),
				"grade");
		assertTrue(iter.hasNext());
		List<Map> r1 = iter.next();
		assertEquals(4, r1.size());
		assertEquals(m1, r1.get(0));
		assertEquals(m2, r1.get(1));
		assertEquals(m3, r1.get(2));
		assertEquals(m4, r1.get(3));

		assertFalse(iter.hasNext());
	}

	public void testIterator_1_3(String grade1, String grade2) {
		Map m1 = createMap("name", "Reshma", "grade", grade1);
		Map m2 = createMap("name", "Sajith", "grade", grade2);
		Map m3 = createMap("name", "Stephania", "grade", grade2);
		Map m4 = createMap("name", "Gloria", "grade", grade2);
		List<Map> srcList = Arrays.asList(m1, m2, m3, m4);
		GroupedMapIterator iter = new GroupedMapIterator(srcList.iterator(),
				"grade");
		assertTrue(iter.hasNext());
		List<Map> r1 = iter.next();
		assertEquals(1, r1.size());
		assertEquals(m1, r1.get(0));

		assertTrue(iter.hasNext());
		List<Map> r2 = iter.next();
		assertEquals(3, r2.size());
		assertEquals(m2, r2.get(0));
		assertEquals(m3, r2.get(1));
		assertEquals(m4, r2.get(2));

		assertFalse(iter.hasNext());
	}

	private Map createMap(String... args) {
		Map returnValue = new HashMap<>();
		for (int a = 0; a < args.length; a += 2) {
			returnValue.put(args[a], args[a + 1]);
		}
		return returnValue;
	}
}
