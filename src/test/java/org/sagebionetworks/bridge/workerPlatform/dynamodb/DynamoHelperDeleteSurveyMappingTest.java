package org.sagebionetworks.bridge.workerPlatform.dynamodb;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.google.common.collect.Iterables;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class DynamoHelperDeleteSurveyMappingTest {
    private static final String APP_ID = "my-app";
    private static final String TABLE_ID_FOO = "table-foo";
    private static final String TABLE_ID_BAR = "table-bar";

    private DynamoHelper helper;
    private Table mockSynapseSurveyTable;

    @BeforeMethod
    public void before() {
        mockSynapseSurveyTable = mock(Table.class);

        helper = new DynamoHelper();
        helper.setDdbSynapseSurveyTablesTable(mockSynapseSurveyTable);
    }

    @Test
    public void noItem() {
        when(mockSynapseSurveyTable.getItem(DynamoHelper.ATTR_APP_ID, APP_ID)).thenReturn(null);
        helper.deleteSynapseSurveyTableMapping(APP_ID, TABLE_ID_FOO);
        verify(mockSynapseSurveyTable, never()).updateItem(any());
    }

    @Test
    public void noSet() {
        when(mockSynapseSurveyTable.getItem(DynamoHelper.ATTR_APP_ID, APP_ID)).thenReturn(new Item());
        helper.deleteSynapseSurveyTableMapping(APP_ID, TABLE_ID_FOO);
        verify(mockSynapseSurveyTable, never()).updateItem(any());
    }

    @Test
    public void setDoesntContainTableId() {
        Item item = new Item().withStringSet(DynamoHelper.ATTR_TABLE_ID_SET, "table-other");
        when(mockSynapseSurveyTable.getItem(DynamoHelper.ATTR_APP_ID, APP_ID)).thenReturn(item);
        helper.deleteSynapseSurveyTableMapping(APP_ID, TABLE_ID_FOO);
        verify(mockSynapseSurveyTable, never()).updateItem(any());
    }

    @Test
    public void normalCase() {
        // Mock DDB table.
        Item item = new Item().withStringSet(DynamoHelper.ATTR_TABLE_ID_SET, TABLE_ID_FOO, TABLE_ID_BAR);
        when(mockSynapseSurveyTable.getItem(DynamoHelper.ATTR_APP_ID, APP_ID)).thenReturn(item);

        // Execute and validate.
        helper.deleteSynapseSurveyTableMapping(APP_ID, TABLE_ID_FOO);

        ArgumentCaptor<UpdateItemSpec> updateItemSpecCaptor = ArgumentCaptor.forClass(UpdateItemSpec.class);
        verify(mockSynapseSurveyTable).updateItem(updateItemSpecCaptor.capture());

        UpdateItemSpec updateItemSpec = updateItemSpecCaptor.getValue();
        assertEquals(updateItemSpec.getUpdateExpression(), "set " + DynamoHelper.ATTR_TABLE_ID_SET + "=:s");

        Collection<KeyAttribute> keyComponents = updateItemSpec.getKeyComponents();
        assertEquals(keyComponents.size(), 1);
        KeyAttribute key = Iterables.getOnlyElement(keyComponents);
        assertEquals(key.getName(), DynamoHelper.ATTR_APP_ID);
        assertEquals(key.getValue(), APP_ID);

        Map<String, Object> valueMap = updateItemSpec.getValueMap();
        assertEquals(valueMap.size(), 1);
        Set<String> tableIdSet = (Set<String>) valueMap.get(":s");
        assertTrue(tableIdSet.contains(TABLE_ID_BAR));
    }

    @Test
    public void removeLastTableId() {
        // Mock DDB table.
        Item item = new Item().withStringSet(DynamoHelper.ATTR_TABLE_ID_SET, TABLE_ID_FOO);
        when(mockSynapseSurveyTable.getItem(DynamoHelper.ATTR_APP_ID, APP_ID)).thenReturn(item);

        // Execute and validate. Update params already tested in a previous test. Just test the submitted table ID set
        // is null.
        helper.deleteSynapseSurveyTableMapping(APP_ID, TABLE_ID_FOO);

        ArgumentCaptor<UpdateItemSpec> updateItemSpecCaptor = ArgumentCaptor.forClass(UpdateItemSpec.class);
        verify(mockSynapseSurveyTable).updateItem(updateItemSpecCaptor.capture());

        UpdateItemSpec updateItemSpec = updateItemSpecCaptor.getValue();
        Map<String, Object> valueMap = updateItemSpec.getValueMap();
        assertEquals(valueMap.size(), 1);
        assertTrue(valueMap.containsKey(":s"));
        assertNull(valueMap.get(":s"));
    }
}
