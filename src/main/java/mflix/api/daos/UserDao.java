package mflix.api.daos;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.apache.logging.log4j.Marker;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import javax.swing.text.html.Option;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!

        try {
            usersCollection.insertOne(user);
            return true;
        } catch (MongoWriteException | MongoWriteConcernException e) {
            throw new IncorrectDaoOperation(e.getMessage());
        }
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

//        UpdateOptions options = new UpdateOptions();
//        options.upsert(true);
//        return usersCollection.updateOne(Filters.eq("email", user.getEmail()), new Document("$set", user), options).wasAcknowledged();

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
        // return false;
        //TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.

        try {
            Session session = new Session();
            session.setJwt(jwt);
            session.setUserId(userId);

            UpdateOptions options = new UpdateOptions().upsert(true);
            return sessionsCollection.updateOne(Filters.eq("user_id", userId), new Document("$set", session), options).wasAcknowledged();

        } catch (MongoException e) {
            log.warn("Unable to create UserSession for userId: `{}` and jwt: '{}' to `Session` object: {} ", userId, jwt, e.getMessage());
            return false;
        }
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        //User user = null;
        //TODO> Ticket: User Management - implement the query that returns the first User object.
        //return user;
        return usersCollection.find(Filters.eq("email", email)).iterator().tryNext();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId
        // return null;
        return sessionsCollection.find(Filters.eq("user_id", userId)).iterator().tryNext();
    }

    public boolean deleteUserSessions(String userId) {
        //TODO> Ticket: User Management - implement the delete user sessions method
        // return false;
        return sessionsCollection.deleteOne(Filters.eq("user_id", userId)).wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        sessionsCollection.deleteOne(Filters.eq("user_id", email));
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
        // return false;
        return usersCollection.deleteOne(Filters.eq("email", email)).wasAcknowledged();

    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.

        if (userPreferences != null) {
            return usersCollection.updateOne(Filters.eq("email", email), set("preferences", userPreferences)).wasAcknowledged();
        } else {
            throw new IncorrectDaoOperation("userPreferences not set!");
        }
    }
}
