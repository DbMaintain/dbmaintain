package org.dbmaintain.database;

import java.util.*;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DatabaseTest {
	
	@Test
	public void sortAccordingToConstraintsTest() {
		List<String> tables = Arrays.asList("A", "B", "C");
		Map<String, Set<String>> childParentRelations = new HashMap<String, Set<String>>();
		Set<String> parents = new HashSet<String>();
		// B is parent of A
		parents.add("B");
		childParentRelations.put("A", parents);
		parents = new HashSet<String>();
		
		// A is parent of C
		parents.add("A");
		childParentRelations.put("C", parents);
		
		List<String> sorted = Database.sortAccordingToConstraints(tables, childParentRelations);
		assertEquals( Arrays.asList("B", "A", "C"), sorted );
	}

}
