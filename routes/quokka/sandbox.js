
const getMeContextFromString = (d) => {

    const splittedString = d.split(",");
    const meContext = splittedString.find((s) => s.includes("MeContext="));
    return meContext.split("=")[1];

}

const searchFilesWithPatternAndMeContext = (dir, pattern, meContext) => {
    const searchPredicate = (entity) => entity.isFile() && entity.name.includes(pattern) && getMeContextFromString(entity.name).includes(meContext);
    return fs
        .readdirSync(dir, { withFileTypes: true })
        .reduce((files, entity) => {
            if (entity.isDirectory()) {
                files.push(...searchFilesWithPatternAndMeContext(path.join(dir, entity.name), pattern, meContext));
            }
            if (entity.isFile() && searchPredicate(entity)) {
                files.push(path.join(dir, entity.name));
            }
            return files;
        }, []);
};

const files = searchFilesWithPatternAndMeContext('/data4', '20230321.03', 'DTKTG0317_JLNKGALORATASTOL');

console.log(files);