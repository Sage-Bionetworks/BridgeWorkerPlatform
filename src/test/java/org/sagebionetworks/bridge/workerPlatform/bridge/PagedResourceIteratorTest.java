package org.sagebionetworks.bridge.workerPlatform.bridge;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.exceptions.BridgeSDKException;
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
        List<String> expectedStudyIds = new ArrayList<>();
        
        mockCall(expectedStudyIds, 0, 10);
        mockCall(expectedStudyIds, 10, 10);
        mockCall(expectedStudyIds, 20, 3);
        mockCall(expectedStudyIds, 30, 0);
        
        PagedResourceIterator<Study> iterator = new PagedResourceIterator<>((ob, ps) -> 
            mockWorkersApi.getSponsoredStudiesForApp("api", "orgId", ob, ps).execute().body().getItems(), 10);
        
        List<String> returnedStudyIds = new ArrayList<>();
        while(iterator.hasNext()) {
            returnedStudyIds.add(iterator.next().getIdentifier());
        }
        assertEquals(returnedStudyIds, expectedStudyIds);
        
        // One more time throws an error
        try {
            iterator.next();
            fail("Should have thrown exeption");
        } catch(NoSuchElementException e) {
        }
    }
    
    @Test
    public void nonRecoverableExceptionDoesNotRetry() throws Exception {
        AtomicInteger integer = new AtomicInteger();
        IOFunction<Integer, Integer, List<Study>> func = (Integer ob, Integer ps) -> {
            integer.incrementAndGet();
            throw new UnsupportedOperationException(); // just something we weren't expecting 
        };
        
        PagedResourceIterator<Study> iterator = new PagedResourceIterator<Study>(func, 10);
        assertEquals(integer.intValue(), 1);
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
    
    @Test
    public void recoverableBridgeSDKExceptionRetries( ) {
        AtomicInteger integer = new AtomicInteger();
        IOFunction<Integer, Integer, List<Study>> func = (Integer ob, Integer ps) -> {
            integer.incrementAndGet();
            throw new BridgeSDKException("This is a request timeout error", 408); 
        };
        
        // just creating an iterator causes it to call the server.
        new PagedResourceIterator<Study>(func, 10, 1);
        assertEquals(integer.intValue(), 4);
    }
    
    @Test
    public void recoverableIOExceptionRetries( ) {
        AtomicInteger integer = new AtomicInteger();
        IOFunction<Integer, Integer, List<Study>> func = (Integer ob, Integer ps) -> {
            integer.incrementAndGet();
            throw new IOException("Some form of network error"); 
        };
        
        // just creating an iterator causes it to call the server.
        new PagedResourceIterator<Study>(func, 10, 1);
        assertEquals(integer.intValue(), 4);
    }
    
    @Test
    public void nonrecoverableBridgeSDKExceptionDoesNotRetry( ) {
        AtomicInteger integer = new AtomicInteger();
        IOFunction<Integer, Integer, List<Study>> func = (Integer ob, Integer ps) -> {
            integer.incrementAndGet();
            throw new BridgeSDKException("This is a not found error", 404); 
        };
        
        // just creating an iterator causes it to call the server.
        new PagedResourceIterator<Study>(func, 10, 1);
        assertEquals(integer.intValue(), 1);
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
