import SwiftUI
import AppKit

extension Notification.Name {
    static let uiResetLayout = Notification.Name("ui.resetLayout")
}

struct ConnectionView: View {
    @ObservedObject var viewModel: ConnectionViewModel
    @EnvironmentObject private var language: LanguageManager
    @State private var searchText = ""
    @State private var presignedAlert: AlertPayload?
    @State private var showUndeleteConfirm = false
    @State private var pendingUndeleteTargets: [S3Object] = []
    @State private var pathInput = ""
    @State private var accessKeyField = ""
    @State private var showTransferStatus = false
    @State private var isDropTargeted = false
    @AppStorage("presignExpiryHours") private var presignExpiryHours: Int = 4
    @AppStorage("ui.mainSplitRatio") private var mainSplitRatio: Double = 0.45
    @AppStorage("ui.inspectorSplitRatio") private var inspectorSplitRatio: Double = 0.72
    @AppStorage("ui.configSplitRatio") private var configSplitRatio: Double = 0.75
    @AppStorage("ui.contentSplitRatio") private var contentSplitRatio: Double = 0.75
    @AppStorage("ui.contentVerticalRatio") private var contentVerticalRatio: Double = 0.625
    @AppStorage("ui.transferSplitRatio") private var transferSplitRatio: Double = 0.6
    @AppStorage("ui.layoutSaved") private var layoutSaved: Bool = false
    @State private var selection = Set<S3Object.ID>()
    @State private var lastSelectedID: S3Object.ID?
    @State private var allowPersistLayout = false
    @State private var didRestoreMainSplit = false
    @State private var didRestoreConfigSplit = false
    @State private var didRestoreContentVerticalSplit = false
    @State private var didRestoreContentSplit = false
    @State private var didRestoreTransferSplit = false
    @State private var didRestoreInspectorSplit = false
    @State private var lastMainLeftWidth: CGFloat = 0
    @State private var lastMainTotalWidth: CGFloat = 0
    @State private var lastConfigTopHeight: CGFloat = 0
    @State private var lastConfigTotalHeight: CGFloat = 0
    @State private var lastContentVerticalTopHeight: CGFloat = 0
    @State private var lastContentVerticalTotalHeight: CGFloat = 0
    @State private var lastContentTopHeight: CGFloat = 0
    @State private var lastContentTotalHeight: CGFloat = 0
    @State private var lastTransferLeftWidth: CGFloat = 0
    @State private var lastTransferTotalWidth: CGFloat = 0
    @State private var lastInspectorLeftWidth: CGFloat = 0
    @State private var lastInspectorTotalWidth: CGFloat = 0
    @AppStorage("ui.maxConfigPanelWidth") private var maxConfigPanelWidthStored: Double = 0
    @AppStorage("ui.maxInspectorPanelWidth") private var maxInspectorPanelWidthStored: Double = 0
    @State private var inspectorTab: Int = 0
    @State private var showCreateBucketWizard = false
    @State private var createBucketName = ""
    @State private var createBucketRegion = ""
    @State private var createBucketVersioning = true
    @State private var createBucketObjectLock = false
    @State private var createBucketError: AlertPayload?
    @State private var toastMessage: String?
    @State private var isCreatingBucket = false
    @State private var showCreateContainerWizard = false
    @State private var createContainerName = ""
    @State private var createContainerAccess: AzurePublicAccess = .privateAccess
    @State private var createContainerError: AlertPayload?
    @State private var isCreatingContainer = false
    @State private var showDeleteContainerConfirm = false
    @State private var deleteContainerName = ""
    @State private var deleteContainerError: AlertPayload?

    init(viewModel: ConnectionViewModel = ConnectionViewModel()) {
        self.viewModel = viewModel
    }

    var body: some View {
        GeometryReader { proxy in
            let totalWidth = max(proxy.size.width, 1)
            let minLeft: CGFloat = 320
            let minRight: CGFloat = 420
            let preferredLeft = totalWidth * CGFloat(mainSplitRatio)
            let maxConfigPanelWidth = maxConfigPanelWidthStored > 0 ? CGFloat(maxConfigPanelWidthStored) : nil
            let cappedLeft = min(preferredLeft, maxConfigPanelWidth ?? preferredLeft)
            let leftWidth = min(max(minLeft, cappedLeft), max(minLeft, totalWidth - minRight))
            let rightWidth = max(minRight, totalWidth - leftWidth)

            HStack(spacing: 0) {
                configPanel
                    .frame(width: leftWidth)
                    .overlay(alignment: .trailing) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(width: 1)
                    }
                content
                    .frame(width: rightWidth)
                    .overlay(alignment: .leading) {
                        Rectangle()
                            .fill(Color(NSColor.separatorColor))
                            .frame(width: 1)
                    }
            }
            .background(
                Color.clear.onAppear {
                    if maxConfigPanelWidthStored <= 0 {
                        let capped = min(max(minLeft, preferredLeft), max(minLeft, totalWidth - minRight))
                        maxConfigPanelWidthStored = Double(capped)
                    }
                }
            )
        }
        .frame(minWidth: 1100, minHeight: 700)
        .alert(item: $presignedAlert) { payload in
            Alert(
                title: Text(payload.title),
                message: Text(payload.message),
                primaryButton: .default(Text(language.t("button.copy"))) {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(payload.message, forType: .string)
                },
                secondaryButton: .cancel(Text(language.t("button.cancel")))
            )
        }
        .alert(item: $createBucketError) { payload in
            Alert(
                title: Text(payload.title),
                message: Text(payload.message),
                dismissButton: .default(Text(language.t("button.ok")))
            )
        }
        .alert(item: $createContainerError) { payload in
            Alert(
                title: Text(payload.title),
                message: Text(payload.message),
                dismissButton: .default(Text(language.t("button.ok")))
            )
        }
        .alert(item: $deleteContainerError) { payload in
            Alert(
                title: Text(payload.title),
                message: Text(payload.message),
                dismissButton: .default(Text(language.t("button.ok")))
            )
        }
        .alert(language.t("alert.confirmDeletionTitle"), isPresented: $showDeleteContainerConfirm) {
            Button(language.t("button.cancel"), role: .cancel) {}
            Button(language.t("button.delete"), role: .destructive) {
                Task { await submitDeleteContainer() }
            }
        } message: {
            Text(String(format: language.t("alert.confirmDeletionBody"), safeFormatArg(deleteContainerName)))
        }
        .alert(language.t("alert.undeleteTitle"), isPresented: $showUndeleteConfirm) {
            Button(language.t("button.cancel"), role: .cancel) {}
            Button(language.t("button.undelete")) {
                let targets = pendingUndeleteTargets
                pendingUndeleteTargets = []
                viewModel.undeleteObjects(targets)
            }
        } message: {
            Text(language.t("alert.undeleteBody"))
        }
        .sheet(isPresented: $showTransferStatus) {
            TransferStatusWindow()
                .environmentObject(viewModel)
                .environmentObject(language)
        }
        .sheet(isPresented: $showCreateBucketWizard) {
            createBucketSheet
                .environmentObject(language)
        }
        .sheet(isPresented: $showCreateContainerWizard) {
            createContainerSheet
                .environmentObject(language)
        }
        .overlay(alignment: .bottomTrailing) {
            if let message = toastMessage {
                Text(message)
                    .font(.caption)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(Color(NSColor.controlBackgroundColor))
                    .cornerRadius(8)
                    .shadow(radius: 3)
                    .padding(12)
            }
        }
        .onAppear {
            accessKeyField = viewModel.accessKey
            restoreSavedLayoutDefaults()
            if !allowPersistLayout {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                    allowPersistLayout = true
                }
            }
        }
        .onChange(of: viewModel.accessKey) { value in
            accessKeyField = value
        }
        .onReceive(NotificationCenter.default.publisher(for: .uiResetLayout)) { _ in
            resetLayout()
        }
    }

    private var configPanel: some View {
        configFormSection
            .onChange(of: viewModel.selectedProfile) { profile in
                guard let profile else { return }
                viewModel.loadProfile(profile)
            }
            .frame(minWidth: 260)
            .background(.ultraThinMaterial)
            .overlay(
                Rectangle()
                    .fill(Color(NSColor.separatorColor))
                    .frame(width: 1),
                alignment: .trailing
            )
    }

    private var configFormSection: some View {
        VStack(spacing: 12) {
            HStack {
                Text(language.t("app.title"))
                    .font(.title2)
                Spacer()
            }
            Text(viewModel.statusMessage)
                .foregroundColor(viewModel.lastStatusCode == nil ? .secondary : .primary)
                .frame(maxWidth: .infinity, alignment: .leading)

            Form {
                Section(language.t("section.profiles")) {
                    HStack(spacing: 8) {
                        TextField(language.t("field.profileName"), text: $viewModel.profileName)
                            .textFieldStyle(.roundedBorder)
                            .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                        Button {
                            let profileName = viewModel.profileName.trimmingCharacters(in: .whitespacesAndNewlines)
                            let displayName = profileName.isEmpty ? language.t("general.default") : profileName
                            let alert = NSAlert()
                            alert.messageText = language.t("alert.deleteProfileTitle")
                            alert.informativeText = String(format: language.t("alert.deleteProfileBody"), displayName)
                            alert.addButton(withTitle: language.t("button.delete"))
                            alert.addButton(withTitle: language.t("button.cancel"))
                            let response = alert.runModal()
                            if response == .alertFirstButtonReturn {
                                viewModel.deleteCurrentProfile()
                            }
                        } label: {
                            Image(systemName: "trash")
                        }
                        .help(language.t("help.deleteProfile"))
                        .disabled(!viewModel.profiles.contains(where: { $0.name == viewModel.profileName.trimmingCharacters(in: .whitespacesAndNewlines) }))
                    }
                    Picker(language.t("field.savedProfiles"), selection: $viewModel.selectedProfile) {
                        Text(language.t("general.none")).tag(ConnectionProfile?.none)
                        ForEach(viewModel.profiles) { profile in
                            Text(profile.name).tag(Optional(profile))
                        }
                    }
                }

                Section(language.t("section.connection")) {
                    TextField(language.t("field.endpoint"), text: $viewModel.endpointURL)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                    TextField(language.t("field.region"), text: $viewModel.region)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                }

                Section(language.t("section.credentials")) {
                    TextField(language.t("field.accessKey"), text: $accessKeyField)
                        .onChange(of: accessKeyField) { value in
                            viewModel.accessKey = value
                        }
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                    SecureField(language.t("field.secretKey"), text: $viewModel.secretKey)
                        .textFieldStyle(.roundedBorder)
                        .shadow(color: Color(NSColor.separatorColor).opacity(0.7), radius: 1, x: 0, y: 0)
                }

                Section(language.t("section.security")) {
                    Toggle(language.t("field.ignoreSSL"), isOn: $viewModel.insecureSSL)
                        .help(language.t("help.ignoreSSL"))
                }
            }
            .formStyle(.grouped)

            HStack {
                Button(language.t("button.saveProfile")) {
                    viewModel.saveProfile()
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button(language.t("button.connect")) {
                    viewModel.testConnection()
                }
                .disabled(viewModel.isBusy)
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .tint(.blue)

                if let status = viewModel.lastStatusCode {
                    Text(String(format: language.t("label.httpStatus"), status))
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }

    private var content: some View {
        VStack(spacing: 0) {
            breadcrumbBar
            Divider()
            GeometryReader { proxy in
                let totalWidth = max(proxy.size.width, 1)
                let minLeft: CGFloat = 520
                let minRight: CGFloat = 320
                let preferredLeft = totalWidth * CGFloat(inspectorSplitRatio)
                let preferredRight = totalWidth - preferredLeft
                let maxRightDefault = min(420, totalWidth * 0.35)
                let maxInspectorPanelWidth = maxInspectorPanelWidthStored > 0 ? CGFloat(maxInspectorPanelWidthStored) : nil
                let maxRight = maxInspectorPanelWidth ?? maxRightDefault
                let cappedRight = min(preferredRight, maxRight)
                let rightWidth = min(max(minRight, cappedRight), max(minRight, totalWidth - minLeft))
                let leftWidth = max(minLeft, totalWidth - rightWidth)

                HStack(spacing: 0) {
                    mainListPanel
                        .frame(width: leftWidth)
                        .overlay(alignment: .trailing) {
                            Rectangle()
                                .fill(Color(NSColor.separatorColor))
                                .frame(width: 1)
                        }
                    inspectorPanel
                        .frame(width: rightWidth)
                        .overlay(alignment: .leading) {
                            Rectangle()
                                .fill(Color(NSColor.separatorColor))
                                .frame(width: 1)
                        }
                }
                .background(
                    Color.clear.onAppear {
                        if maxInspectorPanelWidthStored <= 0 {
                            maxInspectorPanelWidthStored = Double(maxRightDefault)
                        }
                    }
                )
            }
        }
    }

    private var mainListPanel: some View {
        ZStack(alignment: .top) {
            VStack(spacing: 0) {
                objectListHeader
                objectList
            }
            .background(Color.white)
            .onDrop(of: [.fileURL], isTargeted: $isDropTargeted) { providers in
                guard canDropInList else { return false }
                handleDrop(providers)
                return true
            }
            .contextMenu {
                if viewModel.currentBucket == nil {
                    if viewModel.provider == .s3 {
                        Button(language.t("menu.createBucket")) {
                            createBucketName = ""
                            createBucketRegion = viewModel.region
                            createBucketVersioning = true
                            createBucketObjectLock = false
                            showCreateBucketWizard = true
                        }
                    } else if viewModel.canCreateContainer() {
                        Button(language.t("menu.createContainer")) {
                            createContainerName = ""
                            createContainerAccess = .privateAccess
                            showCreateContainerWizard = true
                        }
                    }
                }
            }
            if isDropTargeted && canDropInList {
                RoundedRectangle(cornerRadius: 8)
                    .stroke(Color.accentColor, style: StrokeStyle(lineWidth: 2, dash: [6]))
                    .padding(12)
            }
        }
    }

    private var objectListHeader: some View {
        let showRegionColumn = viewModel.provider == .s3 && viewModel.currentBucket == nil
        return HStack(spacing: 12) {
            Text(language.t("label.name"))
                .frame(maxWidth: .infinity, alignment: .leading)
            Text(language.t("label.size"))
                .frame(width: 90, alignment: .trailing)
            Text(language.t("label.lastModified"))
                .frame(width: 140, alignment: .trailing)
            if showRegionColumn {
                Text(language.t("label.region"))
                    .frame(width: 140, alignment: .trailing)
            } else {
                Text(language.t("label.storageClass"))
                    .frame(width: 120, alignment: .trailing)
            }
            if viewModel.provider == .azureBlob {
                Text(language.t("label.blobType"))
                    .frame(width: 110, alignment: .trailing)
            }
        }
        .font(.caption)
        .foregroundColor(.secondary)
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .background(Color(NSColor.controlBackgroundColor))
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(Color(NSColor.separatorColor))
                .frame(height: 1)
        }
    }

    private var breadcrumbBar: some View {
        HStack(spacing: 8) {
            ForEach(Array(viewModel.breadcrumb.enumerated()), id: \.offset) { index, item in
                Button(item) {
                    viewModel.openBreadcrumb(at: index)
                }
                if index < viewModel.breadcrumb.count - 1 {
                    Text(">")
                        .foregroundColor(.secondary)
                }
            }
            Spacer()
            Button {
                viewModel.goToPreviousPage()
            } label: {
                Image(systemName: "chevron.left")
            }
            .disabled(!viewModel.canPagePrev)
            Button {
                viewModel.goToNextPage()
            } label: {
                Image(systemName: "chevron.right")
            }
            .disabled(!viewModel.canPageNext)
            TextField(language.t("field.pathInput"), text: $pathInput)
                .textFieldStyle(.roundedBorder)
                .frame(width: 260)
                .onSubmit {
                    viewModel.openPathInput(pathInput)
                    pathInput = ""
                }
            Button {
                viewModel.navigateBack()
            } label: {
                Image(systemName: "chevron.backward")
            }
            .disabled(viewModel.currentBucket == nil && viewModel.breadcrumb.count <= 1)
        }
        .padding(10)
    }

    private var objectList: some View {
        List(displayRows, selection: $selection) { row in
            let object = row.object
            let showRegionColumn = viewModel.provider == .s3 && viewModel.currentBucket == nil
            HStack {
                Image(systemName: iconName(for: object))
                VStack(alignment: .leading) {
                    Text(displayKey(for: object))
                        .foregroundColor(row.isChild ? .secondary : .primary)
                        .font(row.isChild ? .subheadline : .body)
                    if object.isDeleteMarker || object.isDeleted || object.isVersioned {
                        Text(versionBadgeText(for: object))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                            .italic()
                    }
                }
                Spacer()
                Text(formatBytes(object.sizeBytes))
                    .foregroundColor(.secondary)
                    .font(row.isChild ? .caption2 : .caption)
                    .frame(width: 90, alignment: .trailing)
                Text(object.lastModified.formatted(date: .abbreviated, time: .shortened))
                    .foregroundColor(.secondary)
                    .font(row.isChild ? .caption2 : .caption)
                    .frame(width: 140, alignment: .trailing)
                if showRegionColumn {
                    Text(displayRegion(for: object))
                        .foregroundColor(.secondary)
                        .font(row.isChild ? .caption2 : .caption)
                        .frame(width: 140, alignment: .trailing)
                } else {
                    Text(displayStorageClass(for: object))
                        .foregroundColor(.secondary)
                        .font(row.isChild ? .caption2 : .caption)
                        .frame(width: 120, alignment: .trailing)
                }
                if viewModel.provider == .azureBlob {
                    Text(displayBlobType(for: object))
                        .foregroundColor(.secondary)
                        .font(row.isChild ? .caption2 : .caption)
                        .frame(width: 110, alignment: .trailing)
                }
            }
            .padding(.leading, row.indent)
            .contentShape(Rectangle())
            .tag(object.id)
            .opacity(rowOpacity(for: row))
            .onTapGesture {
                handleSingleClick(object)
            }
            .onTapGesture(count: 2) {
                viewModel.openObject(object)
            }
            .contextMenu {
                if viewModel.provider == .s3 && object.isVersioned {
                    if object.isDeleteMarker {
                        Button(language.t("menu.removeDeleteMarker")) {
                            viewModel.deleteObjects(selectedTargets(for: object))
                        }
                    } else {
                        Button(language.t("menu.downloadVersion")) {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                        Button(language.t("menu.deleteVersion")) {
                            viewModel.deleteObjects(selectedTargets(for: object))
                        }
                    }
                    if let uri = viewModel.copyURI(for: object) {
                        Button(language.t("menu.copyURI")) {
                            NSPasteboard.general.clearContents()
                            NSPasteboard.general.setString(uri, forType: .string)
                        }
                    }
                } else if viewModel.provider == .azureBlob && (object.isVersioned || object.isDeleted) {
                    if object.isDeleted {
                        Button(language.t("menu.undelete")) {
                            pendingUndeleteTargets = selectedTargets(for: object)
                            showUndeleteConfirm = true
                        }
                    }
                    if let versionId = object.versionId, !versionId.isEmpty {
                        Button(language.t("menu.downloadVersion")) {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                    } else if !object.isDeleted {
                        Button(language.t("menu.download")) {
                            viewModel.downloadObjects(selectedTargets(for: object))
                        }
                    }
                    Button(object.isDeleted ? language.t("menu.permanentDelete") : language.t("menu.delete")) {
                        viewModel.deleteObjects(selectedTargets(for: object))
                    }
                } else {
                    let isBucket = object.contentType == "bucket"
                    let isFolder = object.key.hasSuffix("/")
                    if !isBucket && !isFolder {
                        if viewModel.provider == .azureBlob {
                            Button(language.t("menu.shareLink")) {
                                Task {
                                    if let url = await viewModel.shareLink(for: object, expiresHours: clampedPresignHours) {
                                        NSPasteboard.general.clearContents()
                                        NSPasteboard.general.setString(url, forType: .string)
                                        presentToast(language.t("toast.azureSasCopied"))
                                    } else {
                                        presignedAlert = AlertPayload(title: language.t("menu.shareLink"), message: language.t("error.shareLinkFailed"))
                                    }
                                }
                            }
                        } else {
                            Button(String(format: language.t("menu.presignedURLHours"), clampedPresignHours)) {
                                Task {
                                    if let url = await viewModel.shareLink(for: object, expiresHours: clampedPresignHours) {
                                        NSPasteboard.general.clearContents()
                                        NSPasteboard.general.setString(url, forType: .string)
                                        presentToast(language.t("toast.presignedCopied"))
                                    } else {
                                        presignedAlert = AlertPayload(title: language.t("menu.presignedURL"), message: language.t("menu.presignedURLHelp"))
                                    }
                                }
                            }
                        }
                    }
                    Button(language.t("menu.download")) {
                        viewModel.downloadObjects(selectedTargets(for: object))
                    }
                    if let uri = viewModel.copyURI(for: object) {
                        Button(language.t("menu.copyURI")) {
                            NSPasteboard.general.clearContents()
                            NSPasteboard.general.setString(uri, forType: .string)
                        }
                    }
                    if isBucket {
                        Button(language.t("menu.delete")) {
                            deleteContainerName = object.key
                            showDeleteContainerConfirm = true
                        }
                    } else {
                        Button(language.t("menu.delete")) {
                            viewModel.deleteObjects(selectedTargets(for: object))
                        }
                    }
                }
            }
        }
        .searchable(text: $searchText)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Button {
                    viewModel.setShowVersionsDeleted(!viewModel.showVersionsDeleted)
                } label: {
                    Image(systemName: viewModel.showVersionsDeleted ? "eye.fill" : "eye")
                }
                .help(language.t("help.showVersions"))
            }
            ToolbarItem(placement: .primaryAction) {
                Button {
                    viewModel.refreshCurrentView()
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
                .help(language.t("help.refresh"))
            }
        }
        .onChange(of: selection) { _ in
            let selected = selectedTargets()
            if selected.count == 1, let first = selected.first {
                viewModel.selectObject(first)
            } else {
                viewModel.selectedObject = nil
                viewModel.selectedObjectInfo = nil
            }
        }
    }

    private var propertyPanel: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let info = viewModel.selectedObjectInfo ?? viewModel.selectedObject {
                PropertyRow(title: language.t("label.key"), value: info.key)
                PropertyRow(title: language.t("label.contentType"), value: info.contentType)
                PropertyRow(title: language.t("label.size"), value: "\(info.sizeBytes) bytes")
                PropertyRow(title: language.t("label.lastModified"), value: info.lastModified.formatted(date: .abbreviated, time: .standard))
                PropertyRow(title: language.t("label.etag"), value: info.eTag)
            } else {
                Text(language.t("label.selectObjectDetails"))
                    .foregroundColor(.secondary)
            }
            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var activityPanel: some View {
        VStack(alignment: .leading, spacing: 6) {
            if !viewModel.debugHistory.isEmpty {
                Picker(language.t("label.history"), selection: $viewModel.selectedDebugIndex) {
                    ForEach(0..<viewModel.debugHistory.count, id: \.self) { index in
                        Text(viewModel.debugHistory[index].title).tag(index)
                    }
                }
                .pickerStyle(.menu)
                .font(.caption)
            }
            ScrollView {
                Text(currentDebugText())
                    .font(.caption)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var inspectorPanel: some View {
        VStack(spacing: 8) {
            Picker("", selection: $inspectorTab) {
                Text(language.t("section.objectProperties")).tag(0)
                Text(language.t("section.debugResponse")).tag(1)
            }
            .pickerStyle(.segmented)
            .padding(.horizontal, 8)
            .padding(.top, 6)

            Divider()

            if inspectorTab == 0 {
                propertyPanel
            } else {
                activityPanel
            }
        }
        .background(Color(NSColor.controlBackgroundColor))
    }

    private var filteredObjects: [S3Object] {
        guard !searchText.isEmpty else { return viewModel.objects }
        return viewModel.objects.filter { $0.key.localizedCaseInsensitiveContains(searchText) }
    }

    private var displayRows: [ObjectRowItem] {
        let source = filteredObjects
        guard viewModel.showVersionsDeleted else {
            return source.map { ObjectRowItem(object: $0, indent: 0, isChild: false) }
        }
        return groupedRows(from: source)
    }

    private var displayedObjects: [S3Object] {
        displayRows.map { $0.object }
    }

    private var clampedPresignHours: Int {
        min(max(presignExpiryHours, 1), 168)
    }

    private var canDropInList: Bool {
        viewModel.currentBucket != nil
    }


    private func iconName(for object: S3Object) -> String {
        if object.contentType == "bucket" {
            return "tray.full"
        }
        if object.key.hasSuffix("/") {
            return "folder"
        }
        return "doc"
    }

    private func displayKey(for object: S3Object) -> String {
        let prefix = viewModel.currentPrefix
        if !prefix.isEmpty, object.key.hasPrefix(prefix) {
            return String(object.key.dropFirst(prefix.count))
        }
        return object.key
    }

    private func displayStorageClass(for object: S3Object) -> String {
        let trimmed = object.storageClass.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty || object.key.hasSuffix("/") || object.contentType == "bucket" {
            return "—"
        }
        return trimmed
    }

    private func displayRegion(for object: S3Object) -> String {
        let trimmed = object.region.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty || object.key.hasSuffix("/") || object.contentType != "bucket" {
            return "—"
        }
        return trimmed
    }

    private func displayBlobType(for object: S3Object) -> String {
        let trimmed = object.blobType.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty || object.key.hasSuffix("/") || object.contentType == "bucket" {
            return "—"
        }
        return trimmed
    }

    private func formatBytes(_ bytes: Int) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: Int64(bytes))
    }

    private func selectedTargets(for object: S3Object) -> [S3Object] {
        if selection.isEmpty {
            return [object]
        }
        let selectedSet = Set(selection)
        return viewModel.objects.filter { selectedSet.contains($0.id) }
    }

    private func selectedTargets() -> [S3Object] {
        if selection.isEmpty {
            return []
        }
        let selectedSet = Set(selection)
        return viewModel.objects.filter { selectedSet.contains($0.id) }
    }

    private func handleDrop(_ providers: [NSItemProvider]) {
        var urls: [URL] = []
        let group = DispatchGroup()
        for provider in providers {
            group.enter()
            provider.loadItem(forTypeIdentifier: "public.file-url", options: nil) { item, _ in
                guard let data = item as? Data,
                      let url = URL(dataRepresentation: data, relativeTo: nil) else {
                    DispatchQueue.main.async { group.leave() }
                    return
                }
                DispatchQueue.main.async {
                    urls.append(url)
                    group.leave()
                }
            }
        }
        group.notify(queue: .main) {
            guard !urls.isEmpty else { return }
            showTransferStatus = true
            viewModel.uploadFiles(urls)
        }
    }

    private var createBucketSheet: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(language.t("title.createBucket"))
                .font(.headline)

            VStack(alignment: .leading, spacing: 6) {
                Text(language.t("field.bucketName"))
                TextField("", text: $createBucketName)
                    .textFieldStyle(.roundedBorder)
                if let validation = bucketNameValidationMessage(createBucketName) {
                    Text(validation)
                        .font(.caption)
                        .foregroundColor(.red)
                }
            }

            VStack(alignment: .leading, spacing: 6) {
                Text(language.t("field.region"))
                TextField("", text: $createBucketRegion)
                    .textFieldStyle(.roundedBorder)
                Text(language.t("help.regionRequirement"))
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Toggle(language.t("field.versioning"), isOn: $createBucketVersioning)
            VStack(alignment: .leading, spacing: 4) {
                Toggle(language.t("field.objectLock"), isOn: $createBucketObjectLock)
                Text(language.t("help.objectLockNote"))
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Spacer()

            HStack {
                Spacer()
                Button(language.t("button.cancel")) {
                    showCreateBucketWizard = false
                }
                Button(language.t("button.createBucket")) {
                    Task { await submitCreateBucket() }
                }
                .disabled(bucketNameValidationMessage(createBucketName) != nil || isCreatingBucket)
            }
        }
        .padding(20)
        .frame(width: 420)
        .onAppear {
            if createBucketRegion.isEmpty {
                createBucketRegion = viewModel.region
            }
        }
    }

    private var createContainerSheet: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(language.t("title.createContainer"))
                .font(.headline)

            VStack(alignment: .leading, spacing: 6) {
                Text(language.t("field.containerName"))
                TextField("", text: $createContainerName)
                    .textFieldStyle(.roundedBorder)
                if let validation = containerNameValidationMessage(createContainerName) {
                    Text(validation)
                        .font(.caption)
                        .foregroundColor(.red)
                }
            }

            VStack(alignment: .leading, spacing: 6) {
                Text(language.t("field.publicAccess"))
                Picker("", selection: $createContainerAccess) {
                    Text(language.t("publicAccess.private")).tag(AzurePublicAccess.privateAccess)
                    Text(language.t("publicAccess.blob")).tag(AzurePublicAccess.blob)
                    Text(language.t("publicAccess.container")).tag(AzurePublicAccess.container)
                }
                .pickerStyle(.menu)
            }

            Spacer()

            HStack {
                Spacer()
                Button(language.t("button.cancel")) {
                    showCreateContainerWizard = false
                }
                Button(language.t("button.create")) {
                    Task { await submitCreateContainer() }
                }
                .disabled(containerNameValidationMessage(createContainerName) != nil || isCreatingContainer)
            }
        }
        .padding(20)
        .frame(width: 420)
    }

    private func submitCreateBucket() async {
        let trimmedName = createBucketName.trimmingCharacters(in: .whitespacesAndNewlines)
        if let validation = bucketNameValidationMessage(trimmedName) {
            createBucketError = AlertPayload(title: language.t("alert.createBucketFailedTitle"), message: validation)
            return
        }

        isCreatingBucket = true
        defer { isCreatingBucket = false }

        do {
            let result = try await viewModel.createBucket(
                name: trimmedName,
                region: createBucketRegion,
                enableVersioning: createBucketVersioning,
                enableObjectLock: createBucketObjectLock
            )
            if let status = result.statusCode, status >= 400 {
                let message = (result.responseText?.trimmingCharacters(in: .whitespacesAndNewlines)).flatMap { $0.isEmpty ? nil : $0 } ?? "HTTP \(status)"
                createBucketError = AlertPayload(title: language.t("alert.createBucketFailedTitle"), message: message)
                return
            }
            showCreateBucketWizard = false
            presentToast(String(format: language.t("toast.bucketCreated"), safeFormatArg(trimmedName)))
            viewModel.refreshCurrentView()
        } catch {
            createBucketError = AlertPayload(title: language.t("alert.createBucketFailedTitle"), message: error.localizedDescription)
        }
    }

    private func submitCreateContainer() async {
        let trimmedName = createContainerName.trimmingCharacters(in: .whitespacesAndNewlines)
        if let validation = containerNameValidationMessage(trimmedName) {
            createContainerError = AlertPayload(title: language.t("alert.createContainerFailedTitle"), message: validation)
            return
        }

        isCreatingContainer = true
        defer { isCreatingContainer = false }

        do {
            let result = try await viewModel.createContainer(name: trimmedName, publicAccess: createContainerAccess)
            if let status = result.statusCode, status >= 400 {
                let message = (result.responseText?.trimmingCharacters(in: .whitespacesAndNewlines)).flatMap { $0.isEmpty ? nil : $0 } ?? "HTTP \(status)"
                createContainerError = AlertPayload(title: language.t("alert.createContainerFailedTitle"), message: message)
                return
            }
            showCreateContainerWizard = false
            presentToast(String(format: language.t("toast.containerCreated"), safeFormatArg(trimmedName)))
            viewModel.refreshCurrentView()
        } catch {
            createContainerError = AlertPayload(title: language.t("alert.createContainerFailedTitle"), message: error.localizedDescription)
        }
    }

    private func submitDeleteContainer() async {
        let name = deleteContainerName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return }
        do {
            let result = try await viewModel.deleteBucketOrContainer(name: name)
            if let status = result.statusCode, status >= 400 {
                let message = viewModel.localizedDeleteError(
                    provider: viewModel.provider,
                    responseText: result.responseText,
                    statusCode: result.statusCode
                ) ?? (result.responseText?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "HTTP \(status)")
                deleteContainerError = AlertPayload(title: language.t("alert.deleteFailedTitle"), message: message)
                return
            }
            presentToast(String(format: language.t("toast.resourceDeleted"), safeFormatArg(name)))
            viewModel.refreshCurrentView()
        } catch {
            deleteContainerError = AlertPayload(title: language.t("alert.deleteFailedTitle"), message: error.localizedDescription)
        }
    }

    private func safeFormatArg(_ value: String) -> String {
        value.replacingOccurrences(of: "%", with: "%%")
    }

    private func presentToast(_ message: String) {
        toastMessage = message
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
            if toastMessage == message {
                toastMessage = nil
            }
        }
    }

    private func bucketNameValidationMessage(_ value: String) -> String? {
        let name = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return language.t("error.invalidBucketName") }
        if name.count < 3 || name.count > 63 { return language.t("error.invalidBucketName") }
        if name.hasPrefix(".") || name.hasSuffix(".") || name.hasPrefix("-") || name.hasSuffix("-") {
            return language.t("error.invalidBucketName")
        }
        if name.contains("..") { return language.t("error.invalidBucketName") }
        if name.range(of: "^[a-z0-9][a-z0-9.-]*[a-z0-9]$", options: .regularExpression) == nil {
            return language.t("error.invalidBucketName")
        }
        if name.range(of: "^\\d+\\.\\d+\\.\\d+\\.\\d+$", options: .regularExpression) != nil {
            return language.t("error.invalidBucketName")
        }
        return nil
    }

    private func containerNameValidationMessage(_ value: String) -> String? {
        let name = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return language.t("error.invalidContainerName") }
        if name.count < 3 || name.count > 63 { return language.t("error.invalidContainerName") }
        if name.hasPrefix("-") || name.hasSuffix("-") { return language.t("error.invalidContainerName") }
        if name.range(of: "^[a-z0-9][a-z0-9-]*[a-z0-9]$", options: .regularExpression) == nil {
            return language.t("error.invalidContainerName")
        }
        return nil
    }

    private func handleSingleClick(_ object: S3Object) {
        let flags = NSEvent.modifierFlags.intersection(.deviceIndependentFlagsMask)
        let isShift = flags.contains(.shift)
        let isToggle = flags.contains(.command) || flags.contains(.control)

        if isShift, let anchor = lastSelectedID,
           let rangeIDs = selectionRange(from: anchor, to: object.id) {
            selection = Set(rangeIDs)
        } else if isToggle {
            if selection.contains(object.id) {
                selection.remove(object.id)
            } else {
                selection.insert(object.id)
            }
            lastSelectedID = object.id
        } else {
            selection = [object.id]
            lastSelectedID = object.id
        }

        let selected = selectedTargets()
        if selected.count == 1, let first = selected.first {
            viewModel.selectObject(first)
        } else {
            viewModel.selectedObject = nil
            viewModel.selectedObjectInfo = nil
        }
    }

    private func selectionRange(from start: S3Object.ID, to end: S3Object.ID) -> [S3Object.ID]? {
        let ids = displayedObjects.map { $0.id }
        guard let startIndex = ids.firstIndex(of: start),
              let endIndex = ids.firstIndex(of: end) else {
            return nil
        }
        if startIndex <= endIndex {
            return Array(ids[startIndex...endIndex])
        } else {
            return Array(ids[endIndex...startIndex])
        }
    }

    private func updateMainSplit(leftWidth: CGFloat, totalWidth: CGFloat) {
        guard totalWidth > 0 else { return }
        lastMainLeftWidth = leftWidth
        lastMainTotalWidth = totalWidth
        restoreMainSplitIfNeeded(totalWidth: totalWidth)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(leftWidth / totalWidth), key: "ui.savedMainSplitRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(leftWidth / totalWidth), 0.2), 0.8)
        if ratio != mainSplitRatio {
            mainSplitRatio = ratio
        }
        UserDefaults.standard.set(Double(leftWidth), forKey: "ui.mainSplitWidth")
    }

    private func updateConfigSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        lastConfigTopHeight = topHeight
        lastConfigTotalHeight = totalHeight
        restoreConfigSplitIfNeeded(totalHeight: totalHeight)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(topHeight / totalHeight), key: "ui.savedConfigSplitRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != configSplitRatio {
            configSplitRatio = ratio
        }
        UserDefaults.standard.set(Double(topHeight), forKey: "ui.configSplitTopHeight")
    }

    private func updateContentVerticalSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        lastContentVerticalTopHeight = topHeight
        lastContentVerticalTotalHeight = totalHeight
        restoreContentVerticalSplitIfNeeded(totalHeight: totalHeight)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(topHeight / totalHeight), key: "ui.savedContentVerticalRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != contentVerticalRatio {
            contentVerticalRatio = ratio
        }
        UserDefaults.standard.set(Double(topHeight), forKey: "ui.contentVerticalTopHeight")
    }

    private func updateContentSplit(topHeight: CGFloat, totalHeight: CGFloat) {
        guard totalHeight > 0 else { return }
        lastContentTopHeight = topHeight
        lastContentTotalHeight = totalHeight
        restoreContentSplitIfNeeded(totalHeight: totalHeight)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(topHeight / totalHeight), key: "ui.savedContentSplitRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(topHeight / totalHeight), 0.2), 0.9)
        if ratio != contentSplitRatio {
            contentSplitRatio = ratio
        }
        UserDefaults.standard.set(Double(topHeight), forKey: "ui.contentSplitTopHeight")
    }

    private func updateTransferSplit(leftWidth: CGFloat, totalWidth: CGFloat) {
        guard totalWidth > 0 else { return }
        lastTransferLeftWidth = leftWidth
        lastTransferTotalWidth = totalWidth
        restoreTransferSplitIfNeeded(totalWidth: totalWidth)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(leftWidth / totalWidth), key: "ui.savedTransferSplitRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(leftWidth / totalWidth), 0.2), 0.8)
        if ratio != transferSplitRatio {
            transferSplitRatio = ratio
        }
        UserDefaults.standard.set(Double(leftWidth), forKey: "ui.transferSplitLeftWidth")
    }

    private func updateInspectorSplit(leftWidth: CGFloat, totalWidth: CGFloat) {
        guard totalWidth > 0 else { return }
        lastInspectorLeftWidth = leftWidth
        lastInspectorTotalWidth = totalWidth
        restoreInspectorSplitIfNeeded(totalWidth: totalWidth)
        let wasSaved = layoutSaved
        invalidateLayoutIfNeeded(currentRatio: Double(leftWidth / totalWidth), key: "ui.savedInspectorSplitRatio")
        if wasSaved && layoutSaved { return }
        guard allowPersistLayout, layoutSaved else { return }
        let ratio = min(max(Double(leftWidth / totalWidth), 0.2), 0.85)
        if ratio != inspectorSplitRatio {
            inspectorSplitRatio = ratio
        }
        UserDefaults.standard.set(Double(leftWidth), forKey: "ui.inspectorSplitLeftWidth")
    }

    private func restoreMainSplitIfNeeded(totalWidth: CGFloat) {
        guard layoutSaved, !didRestoreMainSplit, totalWidth > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedMainSplitRatio") as? Double {
            mainSplitRatio = min(max(savedRatio, 0.2), 0.8)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.mainSplitWidth") as? Double {
            let ratio = min(max(saved / Double(totalWidth), 0.2), 0.8)
            mainSplitRatio = ratio
        }
        didRestoreMainSplit = true
    }

    private func restoreConfigSplitIfNeeded(totalHeight: CGFloat) {
        guard layoutSaved, !didRestoreConfigSplit, totalHeight > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedConfigSplitRatio") as? Double {
            configSplitRatio = min(max(savedRatio, 0.2), 0.9)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.configSplitTopHeight") as? Double {
            let ratio = min(max(saved / Double(totalHeight), 0.2), 0.9)
            configSplitRatio = ratio
        }
        didRestoreConfigSplit = true
    }

    private func restoreContentVerticalSplitIfNeeded(totalHeight: CGFloat) {
        guard layoutSaved, !didRestoreContentVerticalSplit, totalHeight > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedContentVerticalRatio") as? Double {
            contentVerticalRatio = min(max(savedRatio, 0.2), 0.9)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.contentVerticalTopHeight") as? Double {
            let ratio = min(max(saved / Double(totalHeight), 0.2), 0.9)
            contentVerticalRatio = ratio
        }
        didRestoreContentVerticalSplit = true
    }

    private func restoreContentSplitIfNeeded(totalHeight: CGFloat) {
        guard layoutSaved, !didRestoreContentSplit, totalHeight > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedContentSplitRatio") as? Double {
            contentSplitRatio = min(max(savedRatio, 0.2), 0.9)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.contentSplitTopHeight") as? Double {
            let ratio = min(max(saved / Double(totalHeight), 0.2), 0.9)
            contentSplitRatio = ratio
        }
        didRestoreContentSplit = true
    }

    private func restoreTransferSplitIfNeeded(totalWidth: CGFloat) {
        guard layoutSaved, !didRestoreTransferSplit, totalWidth > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedTransferSplitRatio") as? Double {
            transferSplitRatio = min(max(savedRatio, 0.2), 0.8)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.transferSplitLeftWidth") as? Double {
            let ratio = min(max(saved / Double(totalWidth), 0.2), 0.8)
            transferSplitRatio = ratio
        }
        didRestoreTransferSplit = true
    }

    private func restoreInspectorSplitIfNeeded(totalWidth: CGFloat) {
        guard layoutSaved, !didRestoreInspectorSplit, totalWidth > 0 else { return }
        if let savedRatio = UserDefaults.standard.object(forKey: "ui.savedInspectorSplitRatio") as? Double {
            inspectorSplitRatio = min(max(savedRatio, 0.2), 0.85)
        } else if let saved = UserDefaults.standard.object(forKey: "ui.inspectorSplitLeftWidth") as? Double {
            let ratio = min(max(saved / Double(totalWidth), 0.2), 0.85)
            inspectorSplitRatio = ratio
        }
        didRestoreInspectorSplit = true
    }

    private func saveLayoutSnapshot() {
        guard lastMainTotalWidth > 0,
              lastInspectorTotalWidth > 0 else {
            layoutSaved = true
            return
        }

        mainSplitRatio = min(max(Double(lastMainLeftWidth / lastMainTotalWidth), 0.2), 0.8)
        inspectorSplitRatio = min(max(Double(lastInspectorLeftWidth / lastInspectorTotalWidth), 0.2), 0.85)

        UserDefaults.standard.set(Double(lastMainLeftWidth), forKey: "ui.mainSplitWidth")
        UserDefaults.standard.set(mainSplitRatio, forKey: "ui.savedMainSplitRatio")
        UserDefaults.standard.set(Double(lastInspectorLeftWidth), forKey: "ui.inspectorSplitLeftWidth")
        UserDefaults.standard.set(inspectorSplitRatio, forKey: "ui.savedInspectorSplitRatio")

        layoutSaved = true
    }

    private func invalidateLayoutIfNeeded(currentRatio: Double, key: String) {
        guard layoutSaved, allowPersistLayout else { return }
        if let saved = UserDefaults.standard.object(forKey: key) as? Double {
            if abs(saved - currentRatio) > 0.01 {
                layoutSaved = false
            }
        }
    }

    private func resetLayout() {
        let defaults = UserDefaults.standard
        let keys = [
            "ui.layoutSaved",
            "ui.savedMainSplitRatio",
            "ui.mainSplitWidth",
            "ui.savedInspectorSplitRatio",
            "ui.inspectorSplitLeftWidth",
            "ui.maxConfigPanelWidth",
            "ui.maxInspectorPanelWidth"
        ]
        keys.forEach { defaults.removeObject(forKey: $0) }

        mainSplitRatio = 0.45
        inspectorSplitRatio = 0.72
        layoutSaved = false
        didRestoreMainSplit = false
        didRestoreInspectorSplit = false
        maxConfigPanelWidthStored = 0
        maxInspectorPanelWidthStored = 0
    }

    private func restoreSavedLayoutDefaults() {
        let defaults = UserDefaults.standard
        var didFindSaved = false

        if let saved = defaults.object(forKey: "ui.savedMainSplitRatio") as? Double {
            mainSplitRatio = min(max(saved, 0.2), 0.8)
            didFindSaved = true
        }
        if let saved = defaults.object(forKey: "ui.savedInspectorSplitRatio") as? Double {
            inspectorSplitRatio = min(max(saved, 0.2), 0.85)
            didFindSaved = true
        }

        if didFindSaved {
            layoutSaved = true
        }
    }

    private func versionBadgeText(for object: S3Object) -> String {
        var labels: [String] = []
        if object.isDeleteMarker {
            labels.append(language.t("badge.deleteMarker"))
        } else if object.isDeleted {
            labels.append(language.t("badge.deleted"))
        } else if object.isVersioned {
            labels.append(language.t("badge.version"))
        }
        if object.isLatest {
            labels.append(language.t("badge.latest"))
        }
        if let versionId = object.versionId, !versionId.isEmpty {
            labels.append(String(format: language.t("badge.id"), String(versionId.prefix(8))))
        }
        return labels.joined(separator: " • ")
    }

    private func currentDebugText() -> String {
        if viewModel.debugHistory.isEmpty {
            return viewModel.debugText.isEmpty ? "(empty)" : viewModel.debugText
        }
        let index = min(max(viewModel.selectedDebugIndex, 0), viewModel.debugHistory.count - 1)
        let text = viewModel.debugHistory[index].text
        return text.isEmpty ? "(empty)" : text
    }

    private func rowOpacity(for row: ObjectRowItem) -> Double {
        var opacity = row.isChild ? 0.7 : 1.0
        if row.object.isDeleteMarker || row.object.isDeleted {
            opacity *= 0.75
        }
        return opacity
    }

    private func groupedRows(from objects: [S3Object]) -> [ObjectRowItem] {
        var versionGroups: [String: [S3Object]] = [:]
        for object in objects where object.isVersioned || object.isDeleteMarker || object.isDeleted {
            versionGroups[object.key, default: []].append(object)
        }

        let groupedKeys = Set(versionGroups.filter { $0.value.count > 1 }.map { $0.key })
        var topLevel: [S3Object] = []
        var childrenByKey: [String: [S3Object]] = [:]

        for object in objects {
            if groupedKeys.contains(object.key) {
                continue
            }
            topLevel.append(object)
        }

        for (key, group) in versionGroups where groupedKeys.contains(key) {
            let sorted = group.sorted { lhs, rhs in
                if lhs.isLatest != rhs.isLatest {
                    return lhs.isLatest && !rhs.isLatest
                }
                if lhs.lastModified != rhs.lastModified {
                    return lhs.lastModified > rhs.lastModified
                }
                return lhs.id.uuidString < rhs.id.uuidString
            }
            guard let parent = sorted.first else { continue }
            topLevel.append(parent)
            let children = Array(sorted.dropFirst()).sorted { $0.lastModified > $1.lastModified }
            childrenByKey[key] = children
        }

        let sortedTop = topLevel.sorted { $0.key < $1.key }
        var rows: [ObjectRowItem] = []
        for object in sortedTop {
            rows.append(ObjectRowItem(object: object, indent: 0, isChild: false))
            if let children = childrenByKey[object.key], !children.isEmpty {
                for child in children {
                    rows.append(ObjectRowItem(object: child, indent: 18, isChild: true))
                }
            }
        }
        return rows
    }
}

private struct TransferStatusWindow: View {
    @EnvironmentObject private var viewModel: ConnectionViewModel
    @EnvironmentObject private var language: LanguageManager
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Text(language.t("section.transferStatus"))
                    .font(.headline)
                Spacer()
                Button(language.t("button.close")) {
                    dismiss()
                }
            }
            .padding()
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    Text(viewModel.transferStatus)
                        .font(.caption)
                        .foregroundColor(.secondary)
                    ProgressView(value: viewModel.transferProgress)
                    Text("\(Int(viewModel.transferProgress * 100))%")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    Divider()
                    ForEach(viewModel.recentTransfers) { item in
                        HStack(spacing: 8) {
                            Text(item.name)
                                .font(.caption)
                                .frame(width: 180, alignment: .leading)
                                .lineLimit(1)
                            ProgressView(value: item.progress)
                                .frame(maxWidth: .infinity)
                            Text("\(Int(item.progress * 100))%")
                                .font(.caption)
                                .frame(width: 40, alignment: .trailing)
                                .foregroundColor(.secondary)
                        }
                    }
                    Spacer()
                }
                .padding()
            }
        }
        .frame(minWidth: 420, minHeight: 300)
    }
}

private struct PropertyRow: View {
    let title: String
    let value: String

    var body: some View {
        HStack(alignment: .firstTextBaseline) {
            Text(title)
                .foregroundColor(.secondary)
                .frame(width: 120, alignment: .leading)
            Text(value.isEmpty ? "-" : value)
                .textSelection(.enabled)
            Spacer()
        }
    }
}

private struct ObjectRowItem: Identifiable {
    let object: S3Object
    let indent: CGFloat
    let isChild: Bool

    var id: UUID { object.id }
}

private struct AlertPayload: Identifiable {
    let id = UUID()
    let title: String
    let message: String
}
