package org.dbmaintain.database;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabaseTest {
	
	@Test
    void sortAccordingToConstraintsTest() {
		List<String> tables = Arrays.asList("A", "B", "C");
		Map<String, Set<String>> childParentRelations = new HashMap<>();
		Set<String> parents = new HashSet<>();
		// B is parent of A
		parents.add("B");
		childParentRelations.put("A", parents);
		parents = new HashSet<>();
		
		// A is parent of C
		parents.add("A");
		childParentRelations.put("C", parents);
		
		List<String> sorted = Database.sortAccordingToConstraints(tables, childParentRelations);
		assertEquals( Arrays.asList("B", "A", "C"), sorted );
	}

}
