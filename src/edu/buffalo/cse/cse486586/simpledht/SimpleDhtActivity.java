/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import java.util.LinkedHashMap;
import java.util.Map;

import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentResolver;
import android.database.Cursor;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    static final String TAG = "DUMP";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        /*
         * findViewById(R.id.button1).setOnClickListener(new OnClickListener() {
         * ContentResolver mContentResolver = getContentResolver(); Uri mUri =
         * buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
         * @Override public void onClick(View v) { Cursor resultCursor =
         * mContentResolver.query(mUri, null, "@", null, null); if (resultCursor
         * == null) { Log.e(TAG, "Result null"); } int keyIndex =
         * resultCursor.getColumnIndex(LocalDHTable.COLUMN_KEY); int valueIndex
         * = resultCursor.getColumnIndex(LocalDHTable.COLUMN_VALUE); if
         * (keyIndex == -1 || valueIndex == -1) { Log.e(TAG, "Wrong columns");
         * resultCursor.close(); } resultCursor.moveToFirst();
         * setQueryCursor(resultCursor); resultCursor.close(); Log.d(TAG,
         * "Local DUMP clicked"); } });
         * findViewById(R.id.button2).setOnClickListener(new OnClickListener() {
         * ContentResolver mContentResolver = getContentResolver(); Uri mUri =
         * buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
         * @Override public void onClick(View v) { Cursor resultCursor =
         * mContentResolver.query(mUri, null, "*", null, null); if (resultCursor
         * == null) { Log.e(TAG, "Result null"); } int keyIndex =
         * resultCursor.getColumnIndex(LocalDHTable.COLUMN_KEY); int valueIndex
         * = resultCursor.getColumnIndex(LocalDHTable.COLUMN_VALUE); if
         * (keyIndex == -1 || valueIndex == -1) { Log.e(TAG, "Wrong columns");
         * resultCursor.close(); } setQueryCursor(resultCursor);
         * resultCursor.close(); Log.d(TAG, "Local DUMP clicked"); } }); }
         * public void setQueryCursor(Cursor cursor) { Map<String,
         * String>queryCursor = new LinkedHashMap<String, String>(); if
         * (cursor.moveToFirst()) { //Log.d("DSPOCK",
         * "Query_All Inside setQueryCursor"); while (!cursor.isAfterLast()) {
         * int keyIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_KEY); int
         * valueIndex = cursor .getColumnIndex(LocalDHTable.COLUMN_VALUE);
         * String returnKey = cursor.getString(keyIndex); String returnValue =
         * cursor.getString(valueIndex); Log.d("DSPOCK",
         * "Query_All Inside setQueryCursor > key=" + returnKey + ", value=" +
         * returnValue); queryCursor.put(returnKey, returnValue);
         * cursor.moveToNext(); } }
         */
    }

    /*
     * private Uri buildUri(String scheme, String authority) { Uri.Builder
     * uriBuilder = new Uri.Builder(); uriBuilder.authority(authority);
     * uriBuilder.scheme(scheme); uriBuilder.appendPath("dht"); return
     * uriBuilder.build(); }
     */

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
