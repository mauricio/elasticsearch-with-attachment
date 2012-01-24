package org.techbot;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


public class Sample {

	private static final String INDEX_NAME = "msdocs";
	private static final String DOCUMENT_TYPE = "doc";
	
	public static void main(String[] args) throws Exception {
		
		try {
			internalMain();
		} catch ( Exception e ) {
			System.out.println( "Found an exception -> " + e.getMessage() );
			e.printStackTrace();
		}
		
	}

	private static void internalMain() throws Exception {
		
		System.out.println( "Started ES client" );
		
		String fileContents = readContent( new File( "llncs.pdf" ) );
		
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

		try {
			DeleteIndexResponse deleteIndexResponse = new DeleteIndexRequestBuilder( client.admin().indices(), INDEX_NAME ).execute().actionGet();
					
			if ( deleteIndexResponse.acknowledged() ) {
				System.out.println( "Deleted index" );
			}	
		} catch ( Exception e ) {
			System.out.println("Index already deleted");
		}
		
		System.out.println("before index create call");
		
		CreateIndexResponse createIndexResponse = new CreateIndexRequestBuilder( client.admin().indices(), INDEX_NAME ).execute().actionGet();
		
		System.out.println("after index create call");
		
		if ( createIndexResponse.acknowledged() ) {
			System.out.println( "created index" );
		}
		
		PutMappingResponse putMappingResponse = 
				new PutMappingRequestBuilder( client.admin().indices() ).setIndices(INDEX_NAME).setType( DOCUMENT_TYPE ).setSource( 
						jsonBuilder()
							.startObject()
								.field("doc")
									.startObject()
										.field( "properties" )
											.startObject()
												.field( "file" )
													.startObject()
														.field( "term_vector", "with_positions_offsets" )
														.field( "store", "yes" )
														.field( "type", "attachment" )
													.endObject()
											.endObject()
									.endObject()
							.endObject() 
						).execute().actionGet();
		
		if ( putMappingResponse.acknowledged() ) {
			System.out.println( "successfully defined mapping" );
		}
		
		IndexResponse indexResponse = client.prepareIndex( INDEX_NAME , DOCUMENT_TYPE, "1")
		        .setSource(jsonBuilder()
		                    .startObject()
		                    	.field( "file", fileContents )
		                    	.field( "modified", new Date() )
		                    	.field( "folder", "/Users/shairon/References" )
		                    	.field( "updated_at", new Date() )
		                    .endObject()
		                  )
		        .execute()
		        .actionGet();
		
		System.out.println( indexResponse );
		
        client.admin().indices().refresh(refreshRequest()).actionGet();
		
		SearchResponse searchResponse = client.prepareSearch( INDEX_NAME )
		        .setSearchType(SearchType.QUERY_AND_FETCH)
		        .setQuery(fieldQuery("file", "refactoring"))
		        .setFrom(0)
		        .setSize(60)
		        .setExplain(true)
		        .execute()
		        .actionGet();
		
		System.out.println( searchResponse );
		
		client.close();		
	}
	
	private static String readContent( File file ) throws Exception {
		
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		InputStream input = new BufferedInputStream( new FileInputStream( file ) );
		
		int read = -1;
		
		while ( ( read = input.read() ) != -1 ) {
			output.write(read);
		}
		
		input.close();
		
		return Base64.encodeBase64String( output.toByteArray()  );
	}
	
}
