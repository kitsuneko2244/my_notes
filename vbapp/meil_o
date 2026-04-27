Imports System.IO
Imports System.Text
Imports System.Data
Imports Oracle.ManagedDataAccess.Client
Imports System.Net.Mail


'''*****************************************************************************
'''  システム名  ：
''' <summary>
''' クラス概要   ：
''' </summary>
''' <remarks>
''' クラス名     ：
''' クラス説明   ：
''' </remarks>
''' <history>
''' 日付 
''' 26/03/28     
''' </history>
'''*****************************************************************************
Module mdlMain

    Private Const strXmlName As String = "SAA1_System.Xml"

    Private strMailServer As String                     ' メールサーバ
    Private strFromAdd As String                        ' Fromメールアドレス
    Private GetConnStr As String                        ' Oracle接続設定
    Private strTopUrl As String                         ' 遷移先URL
    Private strSendMail As String                       ' 実行モード  "DEBUG"指定時はメール送信せずログのみ出力
    Private intMailCnt As Integer = 0
    Private intDataCnt As Integer = 0

    Function Main() As Integer
        Main = 9

        Dim SendTargetData As DataTable
        
        Try
            ConsoleWriteLine("フォローメール送信処理 開始")

            '設定ファイル取得
            If Not InitProc() Then
                Exit Function
            End If

            '送信対象データの取得
            SendTargetData = getTable("YOTEI")
            If SendTargetData.Rows.Count = 0 Then
                ConsoleWriteLine("フォローメール送信対象データなし")
            Else
                'メールの送信
                SendFollowMail(SendTargetData)
                ConsoleWriteLine(String.Format("フォローメール送信対象：[ {0} ]件 メール送信：[ {1} ]件", intDataCnt, intMailCnt))
            End If

            ConsoleWriteLine("フォローメール送信処理 終了")

            Main = 0

        Catch ex As Exception
            'エラーメッセージ
            ConsoleWriteLine("フォローメール送信処理エラー", ex)
        End Try
    End Function

    Private Function InitProc() As Boolean
        InitProc = False

        Try
            '設定ファイルの取得
            Dim myXmlDoc As XDocument = XDocument.Load(My.Application.Info.DirectoryPath.ToString() & "\" & strXmlName)
            Dim query1 = From c In myXmlDoc.Descendants("AppSetting")
            For Each c In query1

                strMailServer = c.Element("MAIL_SERVER").Value          ' メールサーバ
                strFromAdd = c.Element("FROM_ADRRESS").Value            ' Fromメールアドレス
                GetConnStr = c.Element("ORACLECONSTRING").Value         ' Oracle接続設定
                strTopUrl = c.Element("TOP_URL").Value                  ' 遷移先URL
                strSendMail = c.Element("SEND_MAIL").Value              ' 送信モード（"DEBUG"指定時はメール送信せずログのみ出力）

            Next
            InitProc = True

        Catch ex As Exception
            'エラーメッセージ
            ConsoleWriteLine("設定ファイル取得エラー", ex)
        End Try
    End Function

    ''' <summary>
    ''' フォローメール対象データの取得
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function getTable(strType As String, Optional docId As String = "") As DataTable
        Dim strSQL As New StringBuilder
        Try
            Dim OraADP As New OracleDataAdapter
            Dim dtTable As New DataTable

            Using OraCnn As New OracleConnection(GetConnStr)

                Select Case strType
                    Case "YOTEI"
                        strSQL.Length = 0
                        strSQL.AppendLine("WITH")
                        strSQL.AppendLine("    LATEST_STM AS (")
                        strSQL.AppendLine("        SELECT * FROM (")
                        strSQL.AppendLine("            SELECT t.*, ROW_NUMBER() OVER(PARTITION BY PROC_KBN, CHOHYO_CD, KANRI_NO ORDER BY FUKU_NO DESC) AS RK")
                        strSQL.AppendLine("            FROM SATT1010 t")
                        strSQL.AppendLine("        ) WHERE RK = 1")
                        strSQL.AppendLine("    )")
                        strSQL.AppendLine("SELECT STM.DOC_ID")
                        strSQL.AppendLine("    ,STM.KANRI_NO")
                        strSQL.AppendLine("    ,KSY.CHOHYO_CD")
                        strSQL.AppendLine("    ,KLV.DATA_NAME AS KAI_LV_NAME")
                        strSQL.AppendLine("    ,STM.KENMEI")
                        strSQL.AppendLine("    ,STM.HAKKO_DATE")
                        strSQL.AppendLine("    ,CASE WHEN KSY.YOTEI_DATE IS NULL THEN STM.YOTEI_KAKU_KIGEN")
                        strSQL.AppendLine("    ELSE KSY.YOTEI_DATE")
                        strSQL.AppendLine("    END AS YOTEI_DATE")
                        strSQL.AppendLine("    ,TO_CHAR(DAT.WF_KANRYO_DATE, 'YYYY/MM/DD') AS KANRYO_DATE")
                        strSQL.AppendLine("    ,KSY.KATAMEI AS KATAMEI")
                        strSQL.AppendLine("    ,KSQ.DISP_SEQ")
                        strSQL.AppendLine("FROM LATEST_STM STM")
                        strSQL.AppendLine("LEFT OUTER JOIN SAVM0040_DROPDOWN KLV")
                        strSQL.AppendLine("    ON  KLV.PROC_KBN = STM.PROC_KBN")
                        strSQL.AppendLine("    AND KLV.DATA_KEY = STM.KAI_LV")
                        strSQL.AppendLine("    AND KLV.SELECT_ITEM_CD = 'KAI_LV'")
                        strSQL.AppendLine("INNER JOIN SATT1110 KSS")
                        strSQL.AppendLine("    ON  KSS.PROC_KBN = STM.PROC_KBN")
                        strSQL.AppendLine("    AND KSS.GEN_DOC_ID = STM.DOC_ID")
                        strSQL.AppendLine("    AND KSS.STEP_STATUS = '1'")
                        strSQL.AppendLine("INNER JOIN SATT1040 KSY")
                        strSQL.AppendLine("    ON  KSY.DOC_ID = STM.DOC_ID")
                        strSQL.AppendLine("    AND (KSY.NITTEI_MITEI_KBN = '1' OR KSY.YOTEI_DATE IS NOT NULL)")
                        strSQL.AppendLine("LEFT OUTER JOIN SATT1050 JSK")
                        strSQL.AppendLine("    ON  JSK.DOC_ID    = KSY.DOC_ID")
                        strSQL.AppendLine("    AND JSK.KATAMEI   = KSY.KATAMEI")
                        strSQL.AppendLine("    AND JSK.CHOHYO_CD = KSY.CHOHYO_CD")
                        strSQL.AppendLine("    AND NVL(JSK.DEL_FLG, '0') != 'D'")
                        strSQL.AppendLine("    LEFT OUTER JOIN (")
                        strSQL.AppendLine("          SELECT KBT.DOC_ID")
                        strSQL.AppendLine("        , MST.DOC_STATUS")
                        strSQL.AppendLine("        , MST.WF_KANRYO_DATE")
                        strSQL.AppendLine("    FROM SATT2010 KBT")
                        strSQL.AppendLine("    INNER JOIN SATT0010 MST")
                        strSQL.AppendLine("    ON  MST.DOC_ID = KBT.DOC_ID")
                        strSQL.AppendLine("    ) DAT")
                        strSQL.AppendLine("    ON  DAT.DOC_ID = JSK.DOC_ID_KOBETSU")
                        strSQL.AppendLine("LEFT OUTER JOIN SATT1030 KSQ")
                        strSQL.AppendLine("    ON  KSQ.DOC_ID = STM.DOC_ID")
                        strSQL.AppendLine("    AND KSQ.KATAMEI = KSY.KATAMEI")
                        strSQL.AppendLine("WHERE 1 = 1")
                        strSQL.AppendLine("AND  STM.PROC_KBN = 'A1'")
                        strSQL.AppendLine("ORDER BY  STM.KANRI_NO, KSQ.DISP_SEQ")
                    Case "MAIL"
                        strSQL.Length = 0
                        strSQL.AppendLine("SELECT ATESAKI_SEQ")
                        strSQL.AppendLine("      ,ADDRESS_NAME")
                        strSQL.AppendLine("      ,MAIL_ADDRESS")
                        strSQL.AppendLine("FROM SATT0030")
                        strSQL.AppendLine("WHERE 1 = 1")
                        strSQL.AppendFormat("AND  DOC_ID = {0}", docId).AppendLine()
                        strSQL.AppendLine("AND  ATESAKI_KBN = 'NOTICE'")
                    Case "CHOHYO"
                        strSQL.Length = 0
                        strSQL.AppendLine("WITH")
                        strSQL.AppendLine("    LATEST_CSQ AS (")
                        strSQL.AppendLine("        SELECT * FROM (")
                        strSQL.AppendLine("            SELECT t.*, ROW_NUMBER() OVER(PARTITION BY PROC_KBN, CHOHYO_CD ORDER BY STEP_PTN_CD DESC) AS RK")
                        strSQL.AppendLine("            FROM SATM0120 t")
                        strSQL.AppendLine("        ) WHERE RK = 1")
                        strSQL.AppendLine("    )")
                        strSQL.AppendLine("SELECT CHM.PROC_KBN")
                        strSQL.AppendLine("      ,CHM.CHOHYO_CD")
                        strSQL.AppendLine("      ,CHM.CHOHYO_NAME")
                        strSQL.AppendLine("      ,CSQ.DISP_SEQ")
                        strSQL.AppendLine("FROM SATM0010 CHM")
                        strSQL.AppendLine("LEFT OUTER JOIN LATEST_CSQ CSQ")
                        strSQL.AppendLine("    ON  CSQ.PROC_KBN = CHM.PROC_KBN")
                        strSQL.AppendLine("    AND CSQ.CHOHYO_CD = CHM.CHOHYO_CD")
                        strSQL.AppendLine("WHERE 1 = 1")
                        strSQL.AppendLine("AND  CHM.PROC_KBN = 'A1'")
                        strSQL.AppendLine("ORDER BY CSQ.DISP_SEQ")
                End Select
                '送信対象データの取得



                If strSendMail = "DEBUG" Then
                    ConsoleWriteLine(strSQL.ToString())
                End If

                Using OraCmd As New OracleCommand(strSQL.ToString, OraCnn)
                    OraADP.SelectCommand = OraCmd
                    OraADP.Fill(dtTable)
                End Using

            End Using

            Return dtTable

        Catch ex As Exception
            'エラーメッセージ
            ConsoleWriteLine("テーブル取得エラー", ex)
            Return New DataTable
        End Try
    End Function

    ''' <summary>
    ''' フォローメール送信
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function SendFollowMail(ByVal pTable As DataTable) As Boolean
        SendFollowMail = False

        Dim MailMsg As MailMessage = New MailMessage
        Dim smtp As SmtpClient = New SmtpClient
        Dim strSubject As New StringBuilder
        Dim strBody As New StringBuilder
        Dim strLog As New StringBuilder
        Dim mail_to As New ArrayList    ' 取得項目(メール宛先用)
        Dim mail_cc As New ArrayList    ' 取得項目(メール写し用)




        Try

            Dim groups = pTable.AsEnumerable().GroupBy(Function(r) r.Field(Of String)("KANRI_NO"))
            'ステップ管理番号毎のグループ化
            Dim listOfLists As New List(Of List(Of DataRow))()
            Dim keys As List(Of String) = groups.Select(Function(g) g.Key).Where(Function(k) k IsNot Nothing).ToList()
            For Each g In groups
                listOfLists.Add(g.ToList())
            Next

            '帳票リスト
            Dim ChohyoDic As New Dictionary(Of String, String)()
            Dim ChohyoTb As DataTable = getTable("CHOHYO")

            For Each ChohyoRow In ChohyoTb.Rows
                ChohyoDic.Add(ChohyoRow("CHOHYO_CD"), ChohyoRow("CHOHYO_NAME"))
            Next

            Dim myEnc As Encoding = Encoding.GetEncoding("iso-2022-jp")
            smtp.Host = strMailServer
            Dim i As Integer = 0
            For Each list In listOfLists
                '管理番号ごとのグループ
                Dim Stepkanri As New Dictionary(Of String, Date)()
                Dim Stepkatamei As New Dictionary(Of String, Dictionary(Of String, Date))()

                If list(0)("KATAMEI") = "-" Then
                    For Each row As DataRow In list
                        'ここで管理番号毎まで分割
                        MailMsg = New MailMessage

                        '送信元の設定
                        MailMsg.From = New MailAddress(strFromAdd)


                        If row.IsNull("KANRYO_DATE") Then
                            If Stepkanri.ContainsKey(row(2)) Then
                                If Stepkanri(row(2)) > row(6) Then
                                    Stepkanri(row(2)) = row(6)
                                End If
                            Else
                                If Now.AddDays(-1) > row(6) Then
                                    Stepkanri.Add(row(2), row(6))
                                End If
                            End If
                        Else
                            Continue For
                        End If
                    Next
                Else
                    For Each row As DataRow In list
                        'ここで管理番号毎まで分割
                        MailMsg = New MailMessage

                        '送信元の設定
                        MailMsg.From = New MailAddress(strFromAdd)

                        '辞書型構成{形名,{帳票名, 予定日}}
                        If row.IsNull("KANRYO_DATE") Then
                            If Stepkatamei.ContainsKey(row(8)) = False Then

                                Stepkatamei(row(8)) = New Dictionary(Of String, Date)()

                                If Now.AddDays(-1) > row(6) Then
                                    Stepkatamei(row(8)).Add(row(2), row(6))
                                End If

                            Else
                                If Stepkatamei(row(8)).ContainsKey(row(2)) Then
                                    If Stepkatamei(row(8))(row(2)) > row(6) Then
                                        Stepkatamei(row(8))(row(2)) = row(6)
                                    End If
                                Else
                                    If Now.AddDays(-1) > row(6) Then
                                        Stepkatamei(row(8)).Add(row(2), row(6))
                                    End If
                                End If

                            End If
                        End If
                    Next

                End If
                If Stepkanri.Count > 0 Or Stepkatamei.Count > 0 Then

                    Dim AddressTable As DataTable
                    Dim done As Boolean = False 'メールキャンセル
                    AddressTable = getTable("MAIL", list(0)("DOC_ID"))

                    'アドレスが取得できない場合は抜ける
                    If AddressTable.Rows.Count > 0 Then
                        'For Each address In AddressTable.Rows
                        '    MailMsg.To.Add(address("MAIL_ADDRESS"))
                        'Next
                    Else
                        Continue For
                    End If

                    '件名の設定
                    strSubject.Length = 0
                    strSubject.AppendFormat("【作成期限通知】 製品開発管理表：{0}", list(0)("KANRI_NO").ToString)
                    MailMsg.Subject = myEncode(strSubject.ToString, myEnc)

                    '本文の設定
                    strBody.Length = 0

                    strBody.Append("［社外秘］").AppendLine()
                    strBody.AppendFormat("{0}で設定された文書作成予定日を過ぎております。", list(0)("KANRI_NO").ToString).AppendLine()
                    strBody.Append("ご確認の上、速やかに文書作成または計画変更をお願い致します。").AppendLine()
                    strBody.AppendLine("")
                    strBody.AppendFormat("開発レベル : {0}", list(0)("KAI_LV_NAME").ToString).AppendLine()
                    strBody.AppendFormat("開発テーマ : {0}", list(0)("KENMEI").ToString).AppendLine()
                    'strBody.AppendFormat("責任者 : {0}", list(0)("SEKININ_NAME").ToString).AppendLine()
                    strBody.AppendFormat("発行日 : {0:yyyy/MM/dd}", list(0)("HAKKO_DATE")).AppendLine()
                    If Stepkanri.Count > 0 Then

                        strBody.AppendLine("")
                        strBody.Append("作成文書名                            作成予定日").AppendLine()
                        strBody.Append("----------------------------------------------------------").AppendLine()


                        For Each chohyoCd In ChohyoDic.Keys
                            If Stepkanri.ContainsKey(chohyoCd) Then
                                Dim width As Integer = 45 - ChohyoDic(chohyoCd).Length
                                Dim label As String = ChohyoDic(chohyoCd).ToString().PadRight(width)
                                strBody.Append(label).Append(Stepkanri(chohyoCd).ToString("yyyy/MM/dd")).AppendLine()
                                intDataCnt += 1
                            End If
                        Next
                    ElseIf Stepkatamei.Count > 0 Then

                        For Each keyOuter In Stepkatamei.Keys
                            Dim karaCount As Integer = 0
                            If Stepkatamei(keyOuter).Count < 1 Then
                                karaCount += 1
                                If karaCount = Stepkatamei.Count Then
                                    done = True
                                End If
                                Continue For
                            End If
                            strBody.AppendLine("")
                            strBody.Append("----------------------------------------------------------").AppendLine()
                            strBody.AppendLine(keyOuter)
                            strBody.Append("----------------------------------------------------------").AppendLine()
                            strBody.Append("作成文書名                            作成予定日").AppendLine()
                            strBody.Append("----------------------------------------------------------").AppendLine()

                            For Each chohyoCd In ChohyoDic.Keys
                                If Stepkatamei(keyOuter).ContainsKey(chohyoCd) Then
                                    Dim width As Integer = 45 - ChohyoDic(chohyoCd).Length
                                    Dim label As String = ChohyoDic(chohyoCd).ToString().PadRight(width)
                                    strBody.Append(label).Append(Stepkatamei(keyOuter)(chohyoCd).ToString("yyyy/MM/dd")).AppendLine()
                                End If
                            Next

                        Next
                    End If
                    If done = True Then Continue For
                    strBody.AppendLine()
                    strBody.Append("確認URL：").AppendLine()
                    strBody.AppendFormat(strTopUrl, list(0)("DOC_ID").ToString).AppendLine()
                    strBody.Append("----------------------------------------------------------------------------").AppendLine()
                    strBody.Append("このメールは「製品開発管理ワークフローシステム」から自動送信されたものです")

                    MailMsg.IsBodyHtml = False
                    Dim altView As AlternateView = AlternateView.CreateAlternateViewFromString(strBody.ToString, myEnc, System.Net.Mime.MediaTypeNames.Text.Plain)
                    altView.TransferEncoding = System.Net.Mime.TransferEncoding.SevenBit

                    MailMsg.AlternateViews.Add(altView)

                    '設定ファイルが「DEBUG」指定の場合はメール送信しない（ログのみ出力）
                    If strSendMail <> "DEBUG" Then
                        smtp.Send(MailMsg)
                        intMailCnt += 1
                    End If
                    'ログ出力
                    strLog.Length = 0
                    strLog.AppendFormat(" / 件名[{0}]", strSubject.ToString).AppendLine()
                    If Not AddressTable.Rows.Count = 0 Then
                        For Each strToAdd In AddressTable.Rows
                            strLog.AppendFormat(" / 送信先 to[{0}]", strToAdd("MAIL_ADDRESS").ToString).AppendLine()
                        Next
                    End If
                    strLog.AppendLine()
                    strLog.AppendLine(strBody.ToString)
                    ConsoleWriteLine(strLog.ToString, , 0)
                Else
                    Continue For
                End If
            Next
            Return True

        Catch ex As Exception
            'エラーメッセージ
            ConsoleWriteLine("メール送信エラー", ex)
            Throw ex
        End Try
    End Function

    '''*****************************************************************************
    ''' <summary>
    ''' ﾌﾟﾛｼｰｼﾞｬ概要：メールエンコード
    ''' </summary>
    ''' <remarks>
    ''' ﾌﾟﾛｼｰｼﾞｬ名　　： myencode
    ''' ﾌﾟﾛｼｰｼﾞｬ説明　： 文字列をBase64で円コーディングする
    ''' 返り値　　　　： String(変換後文字列)
    ''' </remarks>
    ''' <param name="str">変換文字列</param>
    ''' <param name="enc">エンコーディング</param>
    ''' <returns>String(変換後文字列)</returns>
    '''*****************************************************************************
    Private Function myEncode(ByVal str As String, ByVal enc As System.Text.Encoding) As String
        Dim base64str As String = Convert.ToBase64String(enc.GetBytes(str))
        Return String.Format("=?{0}?B?{1}?=", enc.BodyName, base64str)
    End Function

    ''' <summary>
    ''' コンソール出力メソッド
    ''' </summary>
    ''' <param name="stMessage"></param>
    ''' <param name="pEx"></param>
    ''' <param name="pKbn"></param>
    ''' <remarks></remarks>
    Private Sub ConsoleWriteLine(ByVal stMessage As String, Optional ByVal pEx As Exception = Nothing, Optional ByVal pKbn As Integer = 1)
        If pKbn = 1 Then
            Console.WriteLine(Now.ToString("yyyy/MM/dd HH:mm:ss") & vbTab & stMessage)
        Else
            Console.WriteLine(stMessage)
        End If
        System.Diagnostics.Debug.WriteLine(stMessage)

        If Not IsNothing(pEx) Then
            Console.WriteLine("【エラー】" & pEx.Message)
            System.Diagnostics.Debug.WriteLine("【エラー】" & pEx.Message)
        End If
    End Sub

End Module
