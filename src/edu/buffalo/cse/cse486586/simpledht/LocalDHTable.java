/**
 * @author Dileep Ranganathan
 */

package edu.buffalo.cse.cse486586.simpledht;

import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class LocalDHTable {

    // Database table
    public static final String TABLE_DHT = "dhtable";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";

    // Database creation SQL statement
    private static final String DATABASE_CREATE = "create table "
            + TABLE_DHT
            + "("
            + COLUMN_KEY + " TEXT PRIMARY KEY, "
            + COLUMN_VALUE + " TEXT"
            + ");";

    public static void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    public static void onUpgrade(SQLiteDatabase database, int oldVersion,
            int newVersion) {
        Log.w(LocalDHTable.class.getName(), "Upgrading database from version "
                + oldVersion + " to " + newVersion
                + ", which will destroy all old data");
        database.execSQL("DROP TABLE IF EXISTS " + TABLE_DHT);
        onCreate(database);
    }
}
