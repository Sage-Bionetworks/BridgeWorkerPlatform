package org.sagebionetworks.bridge.workerPlatform.bridge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyList;
import org.sagebionetworks.bridge.workerPlatform.bridge.PagedResourceIterator.IOFunction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import retrofit2.Call;
import retrofit2.Response;

public class PagedResourceIteratorTest extends Mockito {

    @Mock
    ForWorkersApi mockWorkersApi;
    
    @BeforeMethod
    public void beforeMethod() throws IOException {
        MockitoAnnotations.initMocks(this);
    }
    
    private Study createStudy(String id) {
        Study study = new Study();
        study.setIdentifier(id);
        study.setName("Name " + id);
        return study;
    }
    
    @SuppressWarnings("unchecked")
    private void mockCall(List<String> expectedValues, int start, int number) throws Exception {
        List<Study> list1 = new ArrayList<>();
        for (int i=1; i <= number; i++) {
            list1.add(createStudy(""+i));
            expectedValues.add(""+i);
        }
        StudyList page1 = new StudyList();
        setVariableValueInObject(page1, "items", list1);
        
        Call<StudyList> call1 = mock(Call.class);
        when(call1.execute()).thenReturn(Response.success(page1));
        
        when(mockWorkersApi.getSponsoredStudiesForApp("api", "orgId", start, 10)).thenReturn(call1);
    }
    
    @Test
    public void test() throws Exception {
        List<String> expectedValues = new ArrayList<>();
        
        mockCall(expectedValues, 0, 10);
        mockCall(expectedValues, 10, 10);
        mockCall(expectedValues, 20, 3);
        mockCall(expectedValues, 30, 0);
        
        PagedResourceIterator<Study> iterator = new PagedResourceIterator<>((ob, ps) -> 
            mockWorkersApi.getSponsoredStudiesForApp("api", "orgId", ob, ps).execute().body().getItems(), 10);
        
        List<String> retValues = new ArrayList<>();
        while(iterator.hasNext()) {
            retValues.add(iterator.next().getIdentifier());
        }
        assertEquals(retValues, expectedValues);
        
        // One more time throws an error
        try {
            iterator.next();
            fail("Should have thrown exeption");
        } catch(NoSuchElementException e) {
        }
    }
    
    @Test
    public void testFunctionThrowsAnException() throws Exception {
        IOFunction<Integer, Integer, List<Study>> func = (Integer ob, Integer ps) -> {
            throw new IOException(); 
        };
        
        PagedResourceIterator<Study> iterator = new PagedResourceIterator<Study>(func, 10);
        
        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            fail("Should have thrown exeption");
        } catch(NoSuchElementException e) {
            
        }
        
        // Calling these out of order, the result is the same.
        iterator = new PagedResourceIterator<Study>(func, 10);
        try {
            iterator.next();
            fail("Should have thrown exeption");
        } catch(NoSuchElementException e) {
            
        }
        assertFalse(iterator.hasNext());
    }
    
    private static void setVariableValueInObject(Object object, String variable, Object value) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        field.set(object, value);
    }
    
    @SuppressWarnings("rawtypes")
    private static Field getFieldByNameIncludingSuperclasses(String fieldName, Class clazz) {
        Field retValue = null;
        try {
            retValue = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superclass = clazz.getSuperclass();
            if (superclass != null) {
                retValue = getFieldByNameIncludingSuperclasses( fieldName, superclass );
            }
        }
        return retValue;
    }
}
