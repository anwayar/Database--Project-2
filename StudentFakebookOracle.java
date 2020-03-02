package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Map;
import java.util.function.LongBinaryOperator;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

            FirstNameInfo info = new FirstNameInfo();
	        ResultSet rst = stmt.executeQuery("select distinct first_name " +
                    "from " + UsersTable +
                    " where length(first_name) = (select max(length(first_name)) from " + UsersTable + ")"+ 
                    "order by first_name asc");
        	while (rst.next()) {
        		info.addLongName(rst.getString(1));
            }
        	
        	rst = stmt.executeQuery("select distinct first_name " +
                    "from " + UsersTable +
                    " where length(first_name) = (select min(length(first_name)) from " + UsersTable + ")"+
                    "order by first_name asc");
        	while (rst.next()) {
        		info.addShortName(rst.getString(1));
            }
        	
        	rst = stmt.executeQuery("select distinct first_name, count(*) " +
                    "from " + UsersTable +
                    " group by first_name " +
                    "having count(*) = (select max(count(*)) from " + UsersTable + " group by first_name)" + "order by first_name asc");
        	while (rst.next()) {
        		info.addCommonName(rst.getString(1));
        		info.setCommonNameCount(rst.getInt(2));
            }	
        		
            return info;

            // return new FirstNameInfo();                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery("select user_id, first_name, last_name " +
                    "from " + UsersTable +
                    " where user_id not in (select user1_id from " + FriendsTable + " union select user2_id from " + FriendsTable + ")" + "order by user_id asc");
        	while (rst.next()) {
        		Long user_ID = rst.getLong(1);
                	String firstName = rst.getString(2);
                	String lastName = rst.getString(3);
                	results.add(new UserInfo(user_ID, firstName, lastName));
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
        
        
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            String str = "select U.user_id, U.first_name, U.last_name from " + UsersTable + " U, " + CurrentCitiesTable + " C, " + HometownCitiesTable + " H where U.user_id = C.user_id and U.user_id = H.user_id and C.current_city_id is not null and H.hometown_city_id is not null and C.current_city_id <> H.hometown_city_id " + 
            "order by U.user_id asc";
            ResultSet rst = stmt.executeQuery(str);
        	while (rst.next()) {
        		Long user_ID = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                results.add(new UserInfo(user_ID, firstName, lastName));
	    }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            stmt.executeUpdate("create or replace view top_n as " +
            "select photo_id from( " +
            "select P.photo_id, count(*) " +
            "from " + PhotosTable + " P left join " + TagsTable + " T on P.photo_id = T.tag_photo_id " +
            "group by P.photo_id order by 2 desc, 1 asc) " +
            "where rownum <= " + num);
        	
            ResultSet rst = stmt.executeQuery("select top_n.photo_id, P.album_id, A.album_name, P.photo_link, U.user_id, U.first_name, U.last_name " +
            "from top_n, " + PhotosTable + " P, " +  AlbumsTable + " A, " + TagsTable + " T, " + UsersTable + " U " +
            "where top_n.photo_id = P.photo_id and P.album_id = A.album_id and top_n.photo_id = T.tag_photo_id and T.tag_subject_id = U.user_id " +
            "order by 1, 5");

            TaggedPhotoInfo tp = null;
            Long photoId = 0L;
            while (rst.next()) {
                if (!photoId.equals(rst.getLong(1))) {
                    if (tp != null) {
                        results.add(tp);
                    }
                    photoId = rst.getLong(1);
                    Long albumId = rst.getLong(2);
                    String albumName = rst.getString(3);
                    String photoLink = rst.getString(4);
                    PhotoInfo p = new PhotoInfo(photoId, albumId, photoLink, albumName);
                    tp = new TaggedPhotoInfo(p);
        }
                Long user_ID = rst.getLong(5);
                String firstName = rst.getString(6);
                String lastName = rst.getString(7);
                tp.addTaggedUser(new UserInfo(user_ID, firstName, lastName));
        }
            if (tp != null) {
        results.add(tp);
        }
        stmt.executeUpdate("drop view top_n");
 }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            stmt.executeUpdate("create or replace view top_n as " +
    				"select m1_id, m2_id " +
    				"from (select U1.user_id as m1_id, U2.user_id as m2_id, count(T1.tag_photo_id) " +
                    "from " + UsersTable + " U1, " + UsersTable + " U2, " + TagsTable + " T1, " + TagsTable + " T2 " +
                    "where U1.gender = 'male' and U2.gender = 'male' and U1.year_of_birth - U2.year_of_birth <= " + yearDiff + " and U2.year_of_birth - U1.year_of_birth <= " + yearDiff +
                    " and not exists(select * from " + FriendsTable + " F where (F.user1_id = U1.user_id and F.user2_id = U2.user_id)) " +
                    "and U1.user_id = T1.tag_subject_id and U2.user_id = T2.tag_subject_id and T1.tag_photo_id = T2.tag_photo_id and U1.user_id < U2.user_id " +
                    "group by U1.user_id, U2.user_id " +
                    "order by 3 desc, 1 asc, 2 asc)" +
                    "where rownum <= " + num);
    		
    		ResultSet rst = stmt.executeQuery("select T.m1_id, T.m2_id, U1.first_name, U1.last_name, U1.year_of_birth, U2.first_name, U2.last_name, U2.year_of_birth, P.photo_id, P.album_id, A.album_name, P.photo_link " +
                    "from top_n T, " + UsersTable + " U1, " + UsersTable + " U2, " + TagsTable + " T1, " + TagsTable + " T2, " + PhotosTable + " P, " + AlbumsTable + " A " +
                    "where T.m1_id = U1.user_id and T.m2_id = U2.user_id and T.m1_id = T1.tag_subject_id and T.m2_id = T2.tag_subject_id and T1.tag_photo_id = T2.tag_photo_id and T1.tag_photo_id = P.photo_id and P.album_id = A.album_id " +
                    "order by 1 asc, 2 asc");
            Long boy1UserId = null;
            Long boy2UserId = null;
            MatchPair mp = null;
            while (rst.next()) {
                if (boy1UserId == null || (!boy1UserId.equals(rst.getLong(1)) && !boy2UserId.equals(rst.getLong(2)))) {
                    if (mp != null) {
                        results.add(mp);
                    }
                    boy1UserId = rst.getLong(1);
                    String boy1FirstName = rst.getString(3);
                    String boy1LastName = rst.getString(4);
                    UserInfo u1 = new UserInfo(boy1UserId, boy1FirstName, boy1LastName);
                    int boy1Year = rst.getInt(5);
                    boy2UserId = rst.getLong(2);
                    String boy2FirstName = rst.getString(6);
                    String boy2LastName = rst.getString(7);
                    UserInfo u2 = new UserInfo(boy2UserId, boy2FirstName, boy2LastName);
                    int boy2Year = rst.getInt(8);
                    mp = new MatchPair(u1, boy1Year, u2, boy2Year);
                }

                long sharedPhotoId = rst.getLong(9);
                long sharedPhotoAlbumId = rst.getLong(10);
                String sharedPhotoAlbumName = rst.getString(11);
                String sharedPhotoLink = rst.getString(12);
                PhotoInfo p = new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, sharedPhotoLink, sharedPhotoAlbumName);
                mp.addSharedPhoto(p);
            }
            if (mp != null) {
                results.add(mp);
            }
                        
            stmt.executeUpdate("drop view top_n");
            rst.close();
            stmt.close();      
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            stmt.executeUpdate("create or replace view friends_2 as " +
            "select F1.user1_id, F1.user2_id from " + FriendsTable + " F1 " +
            "union select F2.user2_id, F2.user1_id from " + FriendsTable + " F2");
            stmt.executeUpdate("create or replace view friends_common as " +
                    "select F1.user1_id as u1_id, F2.user1_id as u2_id, F1.user2_id as u3_id " +
                    "from friends_2 F1, friends_2 F2 " +
                    "where F1.user1_id < F2.user1_id and F1.user2_id = F2.user2_id");
            stmt.executeUpdate("create or replace view top_n2 as " +
                    "select u1_id, u2_id, quant " +
                    "from (select u1_id, u2_id, count(u3_id) as quant " +
                    "from friends_common " +
                    "where not exists (select * from " + FriendsTable + " F where F.user1_id = u1_id and F.user2_id = u2_id) " +
                    "group by u1_id, u2_id " +
                    "order by 3 desc, 1 asc, 2 asc) " +
                    " fetch first "+num+" rows only");
    
            ResultSet rst = stmt.executeQuery("select T.u1_id, T.u2_id, F.u3_id, U1.first_name, U1.last_name, U2.first_name, U2.last_name, U3.first_name, U3.last_name, T.quant " +
                    "from top_n2 T, friends_common F, " + UsersTable + " U1, " + UsersTable + " U2, " + UsersTable + " U3 " +
                    "where T.u1_id = F.u1_id and T.u2_id = F.u2_id and T.u1_id = U1.user_id and T.u2_id = U2.user_id and F.u3_id = U3.user_id " +
                    "order by 10 desc, 1 asc, 2 asc, 3 asc ");
            
            Long user1_id = null;
            Long user2_id = null;
            UsersPair p = null;
            while (rst.next()) {
                if (user1_id == null || (!user1_id.equals(rst.getLong(1)) || !user2_id.equals(rst.getLong(2)))) {
                    if (p != null) {
                        results.add(p);
                    }
                    user1_id = rst.getLong(1);
                    String user1FirstName = rst.getNString(4);
                    String user1LastName = rst.getNString(5);
                    UserInfo u1 = new UserInfo(user1_id, user1FirstName, user1LastName);
                    user2_id = rst.getLong(2);
                    String user2FirstName = rst.getNString(6);
                    String user2LastName = rst.getNString(7);
                    UserInfo u2 = new UserInfo(user2_id, user2FirstName, user2LastName);
                    p = new UsersPair(u1, u2);
                }
                UserInfo u3 = new UserInfo(rst.getLong(3), rst.getNString(8), rst.getNString(9));
                p.addSharedFriend(u3);
            }
            if (p != null) {
                results.add(p);
            }
            
            stmt.executeUpdate("drop view top_n2");
            stmt.executeUpdate("drop view friends_common");
            stmt.executeUpdate("drop view friends_2");
            rst.close();
            stmt.close();
        }	
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
	        ResultSet rst = stmt.executeQuery("select distinct state_name, count(*) " +
        			"from " + EventsTable + " E left join " + CitiesTable + " C on E.event_city_id = C.city_id " +
        			"where state_name is not null " +
                    "group by state_name " +
                    "having count(*) = (select max(count(*)) from " + EventsTable + " E left join " + CitiesTable + " C on E.event_city_id = C.city_id where state_name is not null group by state_name)");
        	
            EventStateInfo info = new EventStateInfo(-1);
            while (rst.next()) {
                if(rst.isFirst()) {
                    info = new EventStateInfo(rst.getInt(2));
                }
        		info.addState(rst.getString(1));

            }
            return info;                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            UserInfo old = new UserInfo(-1, "ERROR", "ERROR");
            UserInfo young = new UserInfo(-1, "ERROR", "ERROR");
            ResultSet rst = stmt.executeQuery("select * " +
        			"from (select user_id, first_name, last_name " +
                    "from " + UsersTable +
                    " where user_id in (select F1.user1_id from " + FriendsTable + " F1 where F1.user2_id = " + userID + " union select F2.user2_id from " + FriendsTable + " F2 where F2.user1_id = " + userID + ") " +
                    " order by year_of_birth asc, month_of_birth asc, day_of_birth asc, user_id desc) " +
                    "where rownum <= 1");
        	while (rst.next()) {
        		Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                old = new UserInfo(uid, firstName, lastName);
            }
        	
        	rst = stmt.executeQuery("select * " +
        			"from (select user_id, first_name, last_name " +
                    "from " + UsersTable +
                    " where user_id in (select F1.user1_id from " + FriendsTable + " F1 where F1.user2_id = " + userID + " union select F2.user2_id from " + FriendsTable + " F2 where F2.user1_id = " + userID + ") " +
                    " order by year_of_birth desc, month_of_birth desc, day_of_birth desc, user_id asc) " +
                    "where rownum <= 1");
        	while (rst.next()) {
        		Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                young = new UserInfo(uid, firstName, lastName);
            }
           return new AgeInfo(old, young);
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery("select U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
                    "from " + UsersTable + " U1, " + UsersTable + " U2, " + HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 " +
                    "where U1.user_id < U2.user_id and U1.last_name = U2.last_name and U1.user_id = H1.user_id and U2.user_id = H2.user_id and H1.hometown_city_id = H2.hometown_city_id " +
                    "and exists(select * from " + FriendsTable + " F where (F.user1_id = U1.user_id and F.user2_id = U2.user_id) or (F.user1_id = U2.user_id and F.user2_id = U1.user_id)) " +
                    "and U1.year_of_birth - U2.year_of_birth >= -10 and U2.year_of_birth - U1.year_of_birth >= -10 " +
                    "order by U1.user_id asc, U2.user_id asc");
        	
        	while (rst.next()) {
        		Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                UserInfo u1 = new UserInfo(user1_id, user1FirstName, user1LastName);
                UserInfo u2 = new UserInfo(user2_id, user2FirstName, user2LastName);
                SiblingInfo s = new SiblingInfo(u1,u2);
                results.add(s);
            }  
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
