/*
 *  Copyright (C) 2010-2015 Stichting Akvo (Akvo Foundation)
 *
 *  This file is part of Akvo FLOW.
 *
 *  Akvo FLOW is free software: you can redistribute it and modify it under the terms of
 *  the GNU Affero General Public License (AGPL) as published by the Free Software Foundation,
 *  either version 3 of the License or any later version.
 *
 *  Akvo FLOW is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License included below for more details.
 *
 *  The full license text can also be seen at <http://www.gnu.org/licenses/agpl.html>.
 */

package org.waterforpeople.mapping.dataexport;

import java.awt.GraphicsEnvironment;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.SwingUtilities;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.waterforpeople.mapping.app.gwt.client.survey.QuestionDto;
import org.waterforpeople.mapping.app.gwt.client.survey.QuestionDto.QuestionType;
import org.waterforpeople.mapping.app.web.dto.RawDataImportRequest;
import org.waterforpeople.mapping.dataexport.service.BulkDataServiceClient;

import com.gallatinsystems.common.util.StringUtil;
import com.gallatinsystems.framework.dataexport.applet.DataImporter;
import com.gallatinsystems.framework.dataexport.applet.ProgressDialog;

public class RawDataSpreadsheetImporter implements DataImporter {

    private static final Logger log = Logger.getLogger(RawDataSpreadsheetImporter.class);

    private static final String SERVLET_URL = "/rawdatarestapi";
    private static final String DEFAULT_LOCALE = "en";
    public static final String SURVEY_CONFIG_KEY = "surveyId";
    protected static final String KEY_PARAM = "apiKey";
    private static final Map<String, String> SAVING_DATA;
    private static final Map<String, String> COMPLETE;
    private Long surveyId;
    private InputStream stream;
    private ProgressDialog progressDialog;
    private String locale = DEFAULT_LOCALE;
    private ThreadPoolExecutor threadPool;
    private BlockingQueue<Runnable> jobQueue;
    private List<String> errorIds;
    private volatile int currentStep;

    private static final ThreadLocal<DateFormat> DATE_FMT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
        };
    };
    private static final int SIZE_THRESHOLD = 2000 * 400;

    static {
        SAVING_DATA = new HashMap<String, String>();
        SAVING_DATA.put("en", "Saving Data");

        COMPLETE = new HashMap<String, String>();
        COMPLETE.put("en", "Complete");
    }

    /**
     * opens a file input stream using the file passed in and tries to return the first worksheet in
     * that file
     *
     * @param file
     * @return
     * @throws Exception
     */
    public Sheet getDataSheet(File file) throws Exception {
        stream = new PushbackInputStream(new FileInputStream(file));
        Workbook wb = null;
        try {
            wb = WorkbookFactory.create(stream);
        } catch (Exception e) {
        }
        return wb.getSheetAt(0);
    }

    /**
     * closes open input streams
     */
    protected void cleanup() {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    protected void setSurveyId(Map<String, String> criteria) {
        if (criteria != null && criteria.get(SURVEY_CONFIG_KEY) != null) {
            setSurveyId(new Long(criteria.get(SURVEY_CONFIG_KEY).trim()));
        }
    }

    public void runImport(File file, String serverBase, Map<String, String> criteria)
            throws Exception {

        Sheet sheet = getDataSheet(file);
        Map<Long, Long> columnIndexToQuestionId = processHeader(sheet);
        Map<Long, QuestionDto> questionIdToQuestionDto = fetchQuestions(serverBase, criteria);

    }

    /**
     * Parse a raw data report file into a list of InstanceData
     *
     * @param file
     * @return
     */
    public List<InstanceData> parseSheet(Sheet sheet) throws Exception {

        return null;
    }

    /**
     * Parse an instance starting from startRow
     *
     * @param sheet
     * @param startRow
     * @return InstanceData
     */
    public InstanceData parse(Sheet sheet, int startRow) {

        // File layout
        // 0. SurveyedLocaleIdentifier
        // 1. SurveyedLocaleDisplayName
        // 2. DeviceIdentifier
        // 3. SurveyInstanceId
        // 4. CollectionDate
        // 5. SubmitterName
        // 6. SurveyalTime
        // 7. Repeat
        // 8 - N. Questions
        // N + 1. Digest

        Row baseRow = sheet.getRow(startRow);
        String surveyedLocaleIdentifier = baseRow.getCell(0).getStringCellValue();
        String surveyedLocaleDisplayName = baseRow.getCell(1).getStringCellValue();
        String deviceIdentifier = baseRow.getCell(2).getStringCellValue();
        String surveyInstanceId = baseRow.getCell(3).getStringCellValue();
        String collectionDate = baseRow.getCell(4).getStringCellValue();
        String submitterName = baseRow.getCell(5).getStringCellValue();
        String surveyalTime = baseRow.getCell(6).getStringCellValue();
        int firstQuestionColumn = 8;

        int iterations = 1;

        // Count the number maximum number of iterations for this instance
        while (true) {
            Row row = sheet.getRow(startRow + iterations);
            if (row == null || row.getCell(7).getStringCellValue().equals("1")) {
                break;
            }
            iterations++;
        }

        return null;
    }

    /**
     * Return a map of column index -> question id
     *
     * @param sheet
     * @return
     */
    private static Map<Long, Long> processHeader(Sheet sheet) {
        Map<Long, Long> columnIndexToQuestionId = new HashMap<>();
        Row headerRow = sheet.getRow(0);

        for (Cell cell : headerRow) {
            if (cell.getStringCellValue().indexOf("|") > -1) {
                String[] parts = cell.getStringCellValue().split("\\|");
                if (parts[0].trim().length() > 0) {
                    columnIndexToQuestionId.put(Long.valueOf(cell.getColumnIndex()),
                            Long.valueOf(parts[0].trim()));
                }
            }
        }

        return columnIndexToQuestionId;
    }

    /**
     * @return map from question id to QuestionDto
     */
    private static Map<Long, QuestionDto> fetchQuestions(String serverBase,
            Map<String, String> criteria) throws Exception {

        String surveyId = criteria.get("surveyId");
        String apiKey = criteria.get("apiKey");

        Object[] results = BulkDataServiceClient.loadQuestions(surveyId, serverBase, apiKey);

        if (results == null) {
            // TODO proper error reporting
            throw new Exception("Could not fetch questions");
        }
        Map<Long, QuestionDto> questionMap = new HashMap<>();
        for (Entry<String, QuestionDto> entry : ((Map<String, QuestionDto>) results[1]).entrySet()) {
            questionMap.put(Long.valueOf(entry.getKey()), entry.getValue());

        }
        return questionMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeImport(File file, String serverBase,
            Map<String, String> criteria) {
        try {

            int rows = 0;
            errorIds = new ArrayList<String>();
            jobQueue = new LinkedBlockingQueue<Runnable>();
            threadPool = new ThreadPoolExecutor(5, 5, 10, TimeUnit.SECONDS,
                    jobQueue);
            DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
            setSurveyId(criteria);

            Sheet sheet1 = getDataSheet(file);
            if (!GraphicsEnvironment.isHeadless()) {
                progressDialog = new ProgressDialog(sheet1.getLastRowNum(),
                        locale);
                progressDialog.setVisible(true);
            }
            HashMap<Integer, String> questionIDColMap = new HashMap<Integer, String>();
            Object[] results = BulkDataServiceClient.loadQuestions(
                    getSurveyId().toString(), serverBase, criteria.get("apiKey"));
            Map<String, QuestionDto> questionMap = null;

            if (results != null) {
                questionMap = (Map<String, QuestionDto>) results[1];
            }

            boolean hasDurationCol = true;
            boolean setFirstQuestionColumnIdx = true;
            int firstQuestionCol = 0;

            currentStep = 0;
            MessageDigest digest = MessageDigest.getInstance("MD5");
            for (Row row : sheet1) {
                rows++;
                if (row.getRowNum() == 0) {
                    // Process headers
                    for (Cell cell : row) {
                        if (cell.getStringCellValue().indexOf("|") > -1) {
                            if (setFirstQuestionColumnIdx) {
                                firstQuestionCol = cell.getColumnIndex();
                                setFirstQuestionColumnIdx = false;
                            }

                            String[] parts = cell.getStringCellValue().split("\\|");
                            if (parts[0].trim().length() > 0) {
                                questionIDColMap.put(cell.getColumnIndex(), parts[0].trim());
                            }
                        }
                    }
                    continue; // move to next row (data)
                }
                digest.reset();

                String instanceId = null;
                String dateString = null;
                String submitter = null;
                String duration = null;
                String durationSeconds = null;
                StringBuilder sb = new StringBuilder();

                // Headers
                // [identifier, displayName, deviceIdentifier, instanceId, date, submitter,
                // duration, questions...]

                int instanceIdx = firstQuestionCol - 4;
                int dateIdx = firstQuestionCol - 3;
                int submitterIdx = firstQuestionCol - 2;
                int durationIdx = firstQuestionCol - 1;

                sb.append("action="
                        + RawDataImportRequest.SAVE_SURVEY_INSTANCE_ACTION
                        + "&" + RawDataImportRequest.SURVEY_ID_PARAM + "="
                        + getSurveyId() + "&");
                boolean needUpload = true;

                for (Cell cell : row) {
                    if (cell.getColumnIndex() == instanceIdx) {
                        if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                            instanceId = new Double(cell.getNumericCellValue())
                                    .intValue() + "";
                        } else if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                            instanceId = cell.getStringCellValue();
                        }
                        if (instanceId != null) {
                            sb.append(RawDataImportRequest.SURVEY_INSTANCE_ID_PARAM
                                    + "=" + instanceId + "&");
                        }
                    }
                    if (cell.getColumnIndex() == dateIdx) {
                        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                            dateString = cell.getStringCellValue();
                        } else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
                            Date date = HSSFDateUtil.getJavaDate(cell
                                    .getNumericCellValue());
                            dateString = df.format(date);
                        }
                        if (dateString != null) {
                            sb.append(RawDataImportRequest.COLLECTION_DATE_PARAM
                                    + "="
                                    + URLEncoder.encode(dateString, "UTF-8")
                                    + "&");
                        }
                    }
                    if (cell.getColumnIndex() == submitterIdx) {
                        if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                            submitter = cell.getStringCellValue();
                            sb.append("submitter="
                                    + URLEncoder.encode(submitter, "UTF-8")
                                    + "&");
                        }
                    }
                    // Survey Duration
                    if (cell.getColumnIndex() == durationIdx) {
                        if (hasDurationCol) {
                            switch (cell.getCellType()) {
                            // if the cell type is string, we expect hh:mm:ss format
                                case Cell.CELL_TYPE_STRING:
                                    duration = cell.getStringCellValue();
                                    durationSeconds = String.valueOf(durationToSeconds(duration));
                                    digest.update(duration.getBytes());
                                    break;
                                // if the cell type if numeric, we expect a single seconds value
                                case Cell.CELL_TYPE_NUMERIC:
                                    durationSeconds = String.valueOf(cell.getNumericCellValue());
                                    digest.update(durationSeconds.getBytes());
                                    break;
                                default:
                                    durationSeconds = "0";
                                    // don't update the digest, because we want this value to be
                                    // saved.
                                    break;
                            }
                            sb.append("duration="
                                    + URLEncoder.encode(durationSeconds, "UTF-8")
                                    + "&");
                        }
                    }

                    boolean hasValue = false;
                    String qId = questionIDColMap.get(cell.getColumnIndex());

                    if (cell.getColumnIndex() >= firstQuestionCol
                            && qId != null && !qId.trim().equals("")) {
                        QuestionDto question = questionMap.get(questionIDColMap
                                .get(cell.getColumnIndex()));
                        QuestionType type = null;
                        // VALUE is default, it is valid for NUMBER, FREE_TEXT, SCAN, OPTION
                        String typeString = "VALUE";
                        if (question != null) {
                            type = question.getType();
                            if (QuestionType.GEO == type) {
                                typeString = "GEO";
                            } else if (QuestionType.PHOTO == type) {
                                typeString = "IMAGE";
                            } else if (QuestionType.VIDEO == type) {
                                typeString = "VIDEO";
                            } else if (QuestionType.DATE == type) {
                                typeString = "DATE";
                            }
                        } else if (questionIDColMap.get(cell.getColumnIndex())
                                .startsWith("--")) {
                            continue;
                        }

                        String cellVal = parseCellAsString(cell);
                        if (cellVal != null) {
                            cellVal = cellVal.trim();
                            // need to update digest before manipulating the
                            // data
                            digest.update(cellVal.getBytes());
                            if (cellVal.contains("|")) {
                                cellVal = cellVal.replaceAll("\\|", "^^");
                            }
                            if (cellVal.endsWith(".jpg")) {
                                if (cellVal.contains("/")) {
                                    cellVal = cellVal.substring(cellVal
                                            .lastIndexOf("/"));
                                }
                                cellVal = "/sdcard" + cellVal;
                            }
                            if (cellVal.endsWith("UTC")) {
                                try {
                                    cellVal = DATE_FMT.get().parse(cellVal)
                                            .getTime()
                                            + "";
                                } catch (Exception e) {
                                    log.error("bad date format: "
                                            + cellVal + "\n" + e.getMessage(), e);
                                }
                            }
                        }
                        if (cellVal == null) {
                            cellVal = "";
                        }

                        if (type != QuestionType.GEO) {
                            hasValue = true;
                            sb.append(
                                    "questionId="
                                            + questionIDColMap.get(cell
                                                    .getColumnIndex())
                                            + "|value=").append(
                                    cellVal != null ? URLEncoder.encode(
                                            cellVal, "UTF-8") : "");
                        } else {
                            hasValue = true;
                            sb.append("questionId="
                                    + questionIDColMap.get(cell
                                            .getColumnIndex()) + "|value=");
                            if (questionIDColMap.get(cell.getColumnIndex() + 1) != null
                                    && questionIDColMap.get(
                                            cell.getColumnIndex() + 1)
                                            .startsWith("--")) {

                                for (int i = 1; i < 4; i++) {
                                    String nextVal = parseCellAsString(row
                                            .getCell(cell.getColumnIndex() + i));
                                    cellVal += "|"
                                            + (nextVal != null ? nextVal : "");
                                }
                                // if the length of the cellVal is too small, which means there is
                                // no valid info, skip.
                                if (cellVal.length() < 5) {
                                    cellVal = "";
                                }
                                sb.append(cellVal != null ? URLEncoder.encode(
                                        cellVal, "UTF-8") : "");
                            } else {
                                sb.append(cellVal != null ? URLEncoder.encode(
                                        cellVal, "UTF-8") : "");
                            }
                        }

                        if (hasValue) {
                            sb.append("|type=").append(typeString).append("&");
                        }
                    } else if (cell.getColumnIndex() >= firstQuestionCol) {
                        // we should only get here if we have a column that
                        // isn't in the header
                        // as long as the user hasn't messed up the sheet, this
                        // is the md5 digest of the original data
                        try {
                            String md5 = parseCellAsString(cell);
                            String digestVal = StringUtil.toHexString(digest
                                    .digest());
                            if (md5 != null && md5.equals(digestVal)) {
                                needUpload = false;
                            } else if (md5 != null && log.isDebugEnabled()) {
                                log.debug("Row: " + row.getRowNum()
                                        + " MD5: " + digestVal + " orig md5: "
                                        + md5);
                            }
                        } catch (Exception e) {
                            // if we can't handle the md5, then just assume we
                            // need to update the row
                            log.error("Couldn't process md5 for row: "
                                    + row.getRowNum() + " - " + e.getMessage(), e);
                        }
                    }
                }
                if (needUpload) {
                    sendDataToServer(
                            serverBase,
                            instanceId == null ? null
                                    : getResetUrlString(instanceId, dateString, submitter,
                                            durationSeconds),
                            sb.toString(),
                            criteria.get(KEY_PARAM));

                } else {
                    // if we didn't need to upload, then just increment our
                    // progress counter
                    SwingUtilities.invokeLater(new StatusUpdater(currentStep++,
                            SAVING_DATA.get(locale)));
                }
            }
            while (!jobQueue.isEmpty() && threadPool.getActiveCount() > 0) {
                Thread.sleep(5000);
            }
            if (errorIds.size() > 0) {
                log.error("There were ERRORS: ");
                for (String line : errorIds) {
                    log.error(line);
                }
            }
            Thread.sleep(5000);
            log.debug("Updating summaries");
            // now update the summaries
            if ((questionIDColMap.size() * rows) < SIZE_THRESHOLD) {
                invokeUrl(serverBase,
                        "action="
                                + RawDataImportRequest.UPDATE_SUMMARIES_ACTION
                                + "&" + RawDataImportRequest.SURVEY_ID_PARAM
                                + "=" + surveyId, true, criteria.get(KEY_PARAM));
            }

            invokeUrl(serverBase, "action="
                    + RawDataImportRequest.SAVE_MESSAGE_ACTION + "&"
                    + RawDataImportRequest.SURVEY_ID_PARAM + "=" + surveyId,
                    true, criteria.get(KEY_PARAM));

            SwingUtilities.invokeLater(new StatusUpdater(currentStep++,
                    COMPLETE.get(locale), true));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    private Integer durationToSeconds(String duration) {
        if (duration == null || duration.length() == 0)
            return 0;

        // try to parse as integer
        if (!duration.contains(":")) {
            try {
                int seconds = Integer.parseInt(duration);
                return seconds;
            } catch (Exception e) {
                return 0;
            }
        }

        // try do parse as hh:mm:ss
        String[] tokens = duration.split(":");
        if (tokens.length != 3)
            return 0;
        try {
            int hours = Integer.parseInt(tokens[0]);
            int minutes = Integer.parseInt(tokens[1]);
            int seconds = Integer.parseInt(tokens[2]);
            return 3600 * hours + 60 * minutes + seconds;
        } catch (Exception e) {
            return 0;
        }
    }

    private String getResetUrlString(String instanceId, String dateString,
            String submitter, String durationSeconds) throws UnsupportedEncodingException {
        String url = "action="
                + RawDataImportRequest.RESET_SURVEY_INSTANCE_ACTION
                + "&" + RawDataImportRequest.SURVEY_INSTANCE_ID_PARAM
                + "=" + instanceId
                + "&" + RawDataImportRequest.SURVEY_ID_PARAM
                + "=" + getSurveyId()
                + "&" + RawDataImportRequest.COLLECTION_DATE_PARAM
                + "=" + URLEncoder.encode(dateString, "UTF-8")
                + "&" + RawDataImportRequest.SUBMITTER_PARAM
                + "=" + URLEncoder.encode(submitter, "UTF-8");

        // Duration might be missing in old reports
        if (durationSeconds != null) {
            url += "&" + RawDataImportRequest.DURATION_PARAM + "="
                    + URLEncoder.encode(durationSeconds, "UTF-8");
        }

        return url;
    }

    /**
     * handles calling invokeURL twice (once to reset the instance and again to save the new one) as
     * a separate job submitted to the thread pool
     *
     * @param serverBase
     * @param resetUrlString
     * @param saveUrlString
     */
    private void sendDataToServer(final String serverBase,
            final String resetUrlString, final String saveUrlString,
            final String key) {
        threadPool.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    SwingUtilities.invokeLater(new StatusUpdater(currentStep++,
                            SAVING_DATA.get(locale)));
                    if (resetUrlString != null) {
                        invokeUrl(serverBase, resetUrlString, true, key);
                    }
                    invokeUrl(serverBase, saveUrlString, true, key);
                } catch (Exception e) {
                    errorIds.add(saveUrlString);
                    System.err.println("Could not invoke rest services: " + e);
                    e.printStackTrace(System.err);
                }
            }
        });
    }

    /**
     * calls a remote api by posting to the url passed in.
     *
     * @param serverBase
     * @param urlString
     * @throws Exception
     */
    protected void invokeUrl(String serverBase, String urlString,
            boolean shouldSign, String key) throws Exception {

        BulkDataServiceClient.fetchDataFromServer(serverBase + SERVLET_URL,
                urlString, shouldSign, key);
    }

    @Override
    public Map<Integer, String> validate(File file) {
        Map<Integer, String> errorMap = new HashMap<Integer, String>();

        try {
            Sheet sheet = getDataSheet(file);
            Row headerRow = sheet.getRow(0);
            boolean firstQuestionFound = false;

            for (Cell cell : headerRow) {
                String cellValue = cell.getStringCellValue();
                if (firstQuestionFound && !cellValue.matches(".+\\|.+")) {
                    errorMap.put(cell.getColumnIndex(),
                            String.format("The header \"%s\" can not be imported", cellValue));
                    break;
                } else {
                    if (!firstQuestionFound && cellValue.matches("[0-9]+\\|.+")) {
                        firstQuestionFound = true;
                        int idx = cell.getColumnIndex();
                        // idx == 4, non monitoring, old format
                        // idx == 6, monitoring, old format
                        // idx == 7, new format
                        if (!(idx == 4 || idx == 6 || idx == 7)) {
                            errorMap.put(idx, "Found the first question at the wrong column index");
                            break;
                        }
                    }
                }
            }
            if (!firstQuestionFound) {
                errorMap.put(-1, "A question could not be found");
            }

        } catch (Exception e) {
            errorMap.put(-1, e.getMessage());
        }

        return errorMap;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            log.error("Error.\nUsage:\n\tjava org.waterforpeople.mapping.dataexport.RawDataSpreadsheetImporter <file> <serverBase> <surveyId>");
            System.exit(1);
        }
        File file = new File(args[0].trim());
        String serverBaseArg = args[1].trim();
        RawDataSpreadsheetImporter r = new RawDataSpreadsheetImporter();
        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put(SURVEY_CONFIG_KEY, args[2].trim());
        configMap.put("apiKey", args[3].trim());
        r.executeImport(file, serverBaseArg, configMap);
    }

    public Long getSurveyId() {
        return surveyId;
    }

    public void setSurveyId(Long surveyId) {
        this.surveyId = surveyId;
    }

    private String parseCellAsString(Cell cell) {
        String val = null;
        if (cell != null) {
            switch (cell.getCellType()) {
                case Cell.CELL_TYPE_BOOLEAN:
                    val = cell.getBooleanCellValue() + "";
                    break;
                case Cell.CELL_TYPE_NUMERIC:
                    val = cell.getNumericCellValue() + "";
                    break;
                default:
                    val = cell.getStringCellValue();
                    break;
            }
        }
        return val;
    }

    /**
     * Private class to handle updating of the UI thread from our worker thread
     */
    private class StatusUpdater implements Runnable {

        private int step;
        private String msg;
        private boolean isComplete;

        public StatusUpdater(int step, String message) {
            this(step, message, false);
        }

        public StatusUpdater(int step, String message, boolean isComplete) {
            msg = message;
            this.step = step;
            this.isComplete = isComplete;
        }

        @Override
        public void run() {
            if (progressDialog != null) {
                progressDialog.update(step, msg, isComplete);
            }
        }
    }

}
