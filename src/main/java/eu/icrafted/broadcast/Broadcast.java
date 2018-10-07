package eu.icrafted.broadcast;

import com.google.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.spongepowered.api.Game;
import org.spongepowered.api.Sponge;
import org.spongepowered.api.config.ConfigDir;
import org.spongepowered.api.config.DefaultConfig;
import org.spongepowered.api.entity.living.player.Player;
import org.spongepowered.api.event.Listener;
import org.spongepowered.api.event.game.GameReloadEvent;
import org.spongepowered.api.event.game.state.GameStartedServerEvent;
import org.spongepowered.api.event.game.state.GameStoppedServerEvent;
import org.spongepowered.api.plugin.Plugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import ninja.leaping.configurate.commented.CommentedConfigurationNode;
import ninja.leaping.configurate.hocon.HoconConfigurationLoader;
import ninja.leaping.configurate.loader.ConfigurationLoader;
import org.spongepowered.api.scheduler.Task;
import org.spongepowered.api.service.sql.SqlService;
import org.spongepowered.api.text.Text;
import org.spongepowered.api.text.serializer.TextSerializers;

@Plugin(id = "broadcast", name = "iCrafted Broadcast Plugin", version = "0.0.1", description = "Broadcast plugin for global, server and player based messages")
public class Broadcast {
    @Inject
    private Logger logger;

    @Inject
    private Game game;

    @Inject
    @ConfigDir(sharedRoot = false)
    private Path configDir;

    @Inject
    @DefaultConfig(sharedRoot = false)
    private File defConfig;

    @Inject
    @DefaultConfig(sharedRoot = true)
    private ConfigurationLoader<CommentedConfigurationNode> configManager;
    private CommentedConfigurationNode config;

    // sql
    private SqlService sql;
    private Connection conn;

    // scheduler
    private Task taskPlayer;
    private Task taskGlobal;

    @Listener
    public void onServerStart(GameStartedServerEvent event) {
        try {
            logger.info("Starting, iCrafted broadcast...");

            // load configuration
            initConfig();

            // open the SQL connection
            initSqlConnection();

            // load scheduler
            initScheduler();

            logger.info("-> Broadcast module loaded");
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }



    @Listener
    public void onServerStop(GameStoppedServerEvent event) {
        logger.info("Stopping, iCrafted broadcast...");

        // close the connection to the database server
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    @Listener
    public void onReloadPlugins(GameReloadEvent event)
    {
        try {
            // reload configuration
            initConfig();

            // open the SQL connection
            initSqlConnection();

            // load scheduler
            initScheduler();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private void initScheduler()
    {
        List<Long> processedMessages = new ArrayList<>();

        try {
            logger.info("Reloading tasks...");
            if (taskGlobal != null) {
                taskGlobal.cancel();
            }

            if (taskPlayer != null) {
                taskPlayer.cancel();
            }

            String messagePrefix = config.getNode("general", "prefix").getString("");

            // create schedule task
            taskPlayer = game.getScheduler().createTaskBuilder().interval(15, TimeUnit.SECONDS).execute(t -> {
                // get player targeted messages
                for (Player p : game.getServer().getOnlinePlayers()) {
                    try {
                        ResultSet playerMessages = querySql("SELECT id,message,server FROM messages WHERE playeruuid='" + p.getUniqueId().toString() + "'");
                        while (playerMessages.next()) {
                            String server = playerMessages.getString("server");
                            String message = playerMessages.getString("message");
                            long id = playerMessages.getLong("id");

                            if (server == null || server.equalsIgnoreCase(config.getNode("general", "server").toString())) {
                                Text text = TextSerializers.JSON.deserialize(message);
                                p.sendMessage(TextSerializers.FORMATTING_CODE.deserialize(messagePrefix).concat(text));

                                executeSql("DELETE FROM messages WHERE id=" + id);
                            }
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }).submit(this);

            // run every x seconds
            int interval = config.getNode("general", "interval").getInt();

            taskGlobal = game.getScheduler().createTaskBuilder().interval(interval, TimeUnit.SECONDS).execute(t -> {
                // get global and server targeted messages
                try {
                    String query = "SELECT id,message,server FROM messages WHERE " + (processedMessages.size() > 0 ? " id NOT IN(" + StringUtils.join(processedMessages, ",") + ") AND" : "") + " playeruuid IS NULL AND (server='" + config.getNode("general", "server").toString() + "' OR server IS NULL) ORDER BY `order` ASC";

                    ResultSet messages = querySql(query);
                    if(messages != null) {
                        if (messages.getFetchSize() == 0) {
                            processedMessages.clear();
                            messages = querySql(query);
                        }

                        // get the first avaliable mesage
                        messages.first();

                        // get the message from the database
                        String message = messages.getString("message");
                        Text text = TextSerializers.JSON.deserialize(message);
                        game.getServer().getBroadcastChannel().send(TextSerializers.FORMATTING_CODE.deserialize(messagePrefix).concat(text));

                        // add the message to the processed list
                        processedMessages.add(messages.getLong("id"));

                        // OLD CODE, deprecated
                        /*while (messages.next()) {
                            String message = messages.getString("message");

                            Text text = TextSerializers.JSON.deserialize(message);
                            game.getServer().getBroadcastChannel().send(TextSerializers.FORMATTING_CODE.deserialize(messagePrefix).concat(text));

                            processedMessages.add(messages.getLong("id"));
                            break;
                        }*/
                    }
                } catch(SQLException ex) {
                    ex.printStackTrace();
                }
            }).submit(this);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private void initConfig()
    {
        logger.info("-> Config module");
        try {
            // check if configuration exists else create it
            Files.createDirectories(configDir);
            if(!defConfig.exists()) {
                logger.info("Creating configuration file... :)");
                if(!defConfig.createNewFile()) {
                    logger.error("Failed creating file, already exists...");
                }
            }

            // get the configuration manager
            configManager = HoconConfigurationLoader.builder().setFile(defConfig).build();
            config = configManager.load();

            // build configuration
            config.getNode("general", "interval").setValue(config.getNode("general", "interval").getInt(600));
            config.getNode("general", "server").setValue(config.getNode("general", "server").getString(""));
            config.getNode("general", "prefix").setValue(config.getNode("general", "prefix").getString(""));

            config.getNode("database", "server").setValue(config.getNode("database", "server").getString("localhost"));
            config.getNode("database", "username").setValue(config.getNode("database", "username").getString("root"));
            config.getNode("database", "password").setValue(config.getNode("database", "password").getString(""));
            config.getNode("database", "database").setValue(config.getNode("database", "database").getString(""));

            // store the configuration
            configManager.save(config);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void initSqlConnection()
    {
        if(sql == null) {
            sql = Sponge.getServiceManager().provide(SqlService.class).get();
        }

        try {
            conn = sql.getDataSource("jdbc:mysql://" + config.getNode("database", "username").getString() + ":" + config.getNode("database", "password").getString() + "@" + config.getNode("database", "server").getString() + "/" + config.getNode("database", "database").getString()).getConnection();
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    private ResultSet querySql(String query)
    {
        PreparedStatement stmt = null;
        ResultSet results = null;
        try {
            stmt = conn.prepareStatement(query);
            results = stmt.executeQuery();
        } catch(SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
            } catch(SQLException ex) {
                ex.printStackTrace();
            }
        }

        return results;
    }

    private boolean executeSql(String query)
    {
        PreparedStatement stmt = null;
        boolean result = false;

        try {
            stmt = conn.prepareStatement(query);
            stmt.closeOnCompletion();
            result = stmt.execute();
        } catch(SQLException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        return result;
    }
}
